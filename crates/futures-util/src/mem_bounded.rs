// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An mpsc channel bounded by the estimated memory footprint of in-flight messages
//! rather than by message count.

use std::sync::Arc;

use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError, mpsc};

/// The entry that we store in the channel.
///
/// Note: We're storing the size of the entry instead of the RAII permit to
/// save on memory. The permit contains a reference to the semaphore that we
/// don't need to store (16 bytes per entry for permit, vs 4 bytes of the size).
type Entry<T> = (u32, T);

/// The maximum size the channel can have in bytes.
/// That's 2.3 Exabytes.
pub const MAX_SIZE_BYTES: usize = Semaphore::MAX_PERMITS;

/// The maximum size of a single entry in the channel.
/// That's a max of ~4 Gigabytes per item.
pub const MAX_ENTRY_SIZE_BYTES: usize = u32::MAX as usize;

#[derive(Debug, thiserror::Error)]
pub enum SendErrorKind {
    /// Not enough capacity in the channel right now to send the value.
    #[error("insufficient capacity")]
    InsufficientCapacity,

    /// The channel has been closed.
    #[error("channel has been closed")]
    Closed,

    /// The entry is larger than the channel's max capacity. This send will never succeed.
    #[error("entry is langer than the channel's max capacity. This send will never succeed")]
    EntryTooLarge,

    /// The entry is larger than remaining capacity in the permit.
    #[error("entry is larger than remaining capacity in the permit")]
    PermitEntryTooLarge,
}

impl From<TryAcquireError> for SendErrorKind {
    fn from(src: TryAcquireError) -> Self {
        match src {
            TryAcquireError::Closed => Self::Closed,
            TryAcquireError::NoPermits => Self::InsufficientCapacity,
        }
    }
}

#[derive(thiserror::Error)]
pub struct SendError<T> {
    #[source]
    pub kind: SendErrorKind,
    // Returns the element back to the sender
    pub value: T,
}

impl<T> std::fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SendError({:?})", self.kind)
    }
}

impl<T> From<mpsc::error::SendError<T>> for SendError<T> {
    fn from(src: mpsc::error::SendError<T>) -> Self {
        Self {
            kind: SendErrorKind::Closed,
            value: src.0,
        }
    }
}

impl From<tokio::sync::AcquireError> for SendError<()> {
    fn from(_src: tokio::sync::AcquireError) -> Self {
        Self {
            kind: SendErrorKind::Closed,
            value: (),
        }
    }
}

impl<T> From<mpsc::error::SendError<Entry<T>>> for SendError<T> {
    fn from(src: mpsc::error::SendError<Entry<T>>) -> Self {
        Self {
            kind: SendErrorKind::Closed,
            value: src.0.1,
        }
    }
}

impl From<TryAcquireError> for SendError<()> {
    fn from(src: TryAcquireError) -> Self {
        Self {
            kind: SendErrorKind::from(src),
            value: (),
        }
    }
}

pub trait EstimatedSize {
    /// The estimated size of the value in bytes.
    fn estimated_size(&self) -> u32;
}

/// Creates a channel with the given size in bytes.
pub fn channel<T: EstimatedSize>(size: usize) -> (Sender<T>, Receiver<T>) {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(size));
    let (tx, rx) = mpsc::unbounded_channel();
    (
        Sender {
            max_capacity: size,
            semaphore: Arc::clone(&semaphore),
            tx,
        },
        Receiver { semaphore, rx },
    )
}

/// A reservation of channel capacity for a single message.
///
/// The reserved bytes are held until the message is sent (and later consumed by the
/// receiver) or until the permit is dropped.
pub struct Permit<'a, T: EstimatedSize> {
    permit: SemaphorePermit<'a>,
    tx: &'a mpsc::UnboundedSender<Entry<T>>,
}

impl<'a, T: EstimatedSize> Permit<'a, T> {
    /// Sends `value` on the channel using the reserved capacity.
    ///
    /// Sending a value bigger than the reserved capacity will fail and return the value back.
    /// Sending a value smaller than the reserved capacity will return the difference to the channel capacity.
    /// If the receiver has been dropped in the meantime, the send will error and the
    /// the value will be returned.
    pub fn send(mut self, value: T) -> Result<(), SendError<T>> {
        let size = value.estimated_size();
        if (size as usize) > self.permit.num_permits() {
            std::hint::cold_path();
            // The caller didn't reserve enough capacity for this message.
            return Err(SendError {
                kind: SendErrorKind::PermitEntryTooLarge,
                value,
            });
        } else if (size as usize) < self.permit.num_permits() {
            // Let's refund the difference by splitting the permit and releasing it.
            let _ = self
                .permit
                .split(self.permit.num_permits() - size as usize)
                .expect("we have enough permits");
        }

        // Now forget the permits here, and we'll add them later when we consume the message.
        self.permit.forget();
        Ok(self.tx.send((size, value))?)
    }
}

/// A reservation of channel capacity for multiple messages.
///
/// Each [`Permits::send`] consumes the sent message's estimated size from the reservation.
pub struct Permits<'a, T: EstimatedSize> {
    permit: SemaphorePermit<'a>,
    tx: &'a mpsc::UnboundedSender<Entry<T>>,
}

impl<'a, T: EstimatedSize> Permits<'a, T> {
    /// Sends `value`, consuming its estimated size from the reserved capacity.
    ///
    /// Sending a value bigger than the reserved capacity will fail and return the value back.
    /// If the receiver has been dropped in the meantime, the send will error and the
    /// the value will be returned.
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        let size = value.estimated_size();
        if (size as usize) > self.permit.num_permits() {
            std::hint::cold_path();
            // The caller didn't reserve enough capacity for this message.
            return Err(SendError {
                kind: SendErrorKind::PermitEntryTooLarge,
                value,
            });
        }
        // Consume the permits here, and forget them. We'll add them back later when we consume the message.
        self.permit
            .split(size as usize)
            .expect("we have enough permits")
            .forget();
        Ok(self.tx.send((size, value))?)
    }

    /// The number of bytes left in this permit.
    pub fn capacity(&self) -> usize {
        self.permit.num_permits()
    }
}

pub struct Sender<T: EstimatedSize> {
    max_capacity: usize,
    semaphore: Arc<Semaphore>,
    tx: mpsc::UnboundedSender<Entry<T>>,
}

impl<T: EstimatedSize> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            max_capacity: self.max_capacity,
            semaphore: Arc::clone(&self.semaphore),
            tx: self.tx.clone(),
        }
    }
}

impl<T: EstimatedSize> Sender<T> {
    /// Sends the value `t` to the channel, and waits until there is enough capacity to hold
    /// the estimated size of `t`.
    /// Fails if the channel has been closed.
    /// Sending a value bigger than the max capacity of the channel will fail with SendErrorKind::EntryTooLarge.
    pub async fn send(&self, t: T) -> Result<(), SendError<T>> {
        let size = t.estimated_size();
        if (size as usize) > self.max_capacity {
            std::hint::cold_path();
            return Err(SendError {
                kind: SendErrorKind::EntryTooLarge,
                value: t,
            });
        }
        let Ok(permit) = self.semaphore.acquire_many(size).await else {
            // Semaphore has been closed, this means the channel is as well. Return send error
            return Err(SendError {
                kind: SendErrorKind::Closed,
                value: t,
            });
        };
        // Forget the permit here, and we'll add it later when we consume the message.
        permit.forget();
        Ok(self.tx.send((size, t))?)
    }

    /// Tries to reserved `n` bytes from the channel capacity.
    /// Fails if there isn't enough capacity or the channel has been closed.
    /// Reserving a value bigger than the max capacity of the channel will fail with SendErrorKind::EntryTooLarge.
    pub fn try_reserve(&self, n: u32) -> Result<Permit<'_, T>, SendError<()>> {
        if (n as usize) > self.max_capacity {
            std::hint::cold_path();
            return Err(SendError {
                kind: SendErrorKind::EntryTooLarge,
                value: (),
            });
        }
        Ok(self.semaphore.try_acquire_many(n).map(|permit| Permit {
            permit,
            tx: &self.tx,
        })?)
    }

    /// Reserves `n` bytes from the channel capacity. Completes where there is enough capacity.
    /// Fails if the channel has been closed.
    /// Reserving a value bigger than the max capacity of the channel will fail with SendErrorKind::EntryTooLarge.
    pub async fn reserve(&self, n: u32) -> Result<Permit<'_, T>, SendError<()>> {
        if (n as usize) > self.max_capacity {
            std::hint::cold_path();
            return Err(SendError {
                kind: SendErrorKind::EntryTooLarge,
                value: (),
            });
        }
        Ok(self.semaphore.acquire_many(n).await.map(|permit| Permit {
            permit,
            tx: &self.tx,
        })?)
    }

    /// Returns a [`Permits`] that reserves `n` bytes from the channel capacity. Or an error
    /// if the channel has been closed or doesn't have enough capacity.
    /// Reserving a value bigger than the max capacity of the channel will fail with SendErrorKind::EntryTooLarge.
    pub fn try_reserve_many(&self, n: u32) -> Result<Permits<'_, T>, SendError<()>> {
        if (n as usize) > self.max_capacity {
            std::hint::cold_path();
            return Err(SendError {
                kind: SendErrorKind::EntryTooLarge,
                value: (),
            });
        }
        Ok(self.semaphore.try_acquire_many(n).map(|permit| Permits {
            permit,
            tx: &self.tx,
        })?)
    }

    /// Returns a [`Permits`] that reserves `n` bytes from the channel capacity. Or an error
    /// if the channel has been closed.
    /// Reserving a value bigger than the max capacity of the channel will fail with SendErrorKind::EntryTooLarge.
    pub async fn reserve_many(&self, n: u32) -> Result<Permits<'_, T>, SendError<()>> {
        if (n as usize) > self.max_capacity {
            std::hint::cold_path();
            return Err(SendError {
                kind: SendErrorKind::EntryTooLarge,
                value: (),
            });
        }
        Ok(self.semaphore.acquire_many(n).await.map(|permit| Permits {
            permit,
            tx: &self.tx,
        })?)
    }

    /// Completes when the receiver has dropped.
    pub async fn closed(&self) {
        self.tx.closed().await
    }

    /// The current capacity of the channel in bytes.
    /// Capacity gets consumed as messages get sent to the channel
    /// and returned when a message are consumed from the channel.
    pub fn capacity(&self) -> usize {
        self.semaphore.available_permits()
    }
}

pub struct Receiver<T> {
    semaphore: Arc<Semaphore>,
    rx: mpsc::UnboundedReceiver<Entry<T>>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecvBatch {
    /// The number of elements added to the buffer.
    pub count: usize,
    /// The total estimated size of the elements added to the buffer.
    pub bytes: usize,
    /// The size of the next element when it could not fit within the byte limit.
    pub next_entry_bytes: Option<usize>,
}

impl<T> Receiver<T> {
    /// Receives a value from the channel. Freeing up the capacity held by that item.
    pub async fn recv(&mut self) -> Option<T> {
        self.rx.recv().await.map(|(size, t)| {
            self.semaphore.add_permits(size as usize);
            t
        })
    }

    pub fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        self.rx.try_recv().map(|(size, t)| {
            self.semaphore.add_permits(size as usize);
            t
        })
    }

    pub fn close(&mut self) {
        self.rx.close();
        self.semaphore.close();
    }

    pub fn is_closed(&self) -> bool {
        self.rx.is_closed()
    }

    /// The number of messages currently buffered in the channel.
    pub fn len(&self) -> usize {
        self.rx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
    }
}

/// A receiver that receives messages in batches bounded by byte and element counts.
pub struct BatchReceiver<T> {
    receiver: Receiver<T>,
    pending: Option<Entry<T>>,
}

impl<T> BatchReceiver<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        Self {
            receiver,
            pending: None,
        }
    }

    fn recv_many_internal(
        &mut self,
        buf: &mut Vec<T>,
        max_count: usize,
        max_bytes: usize,
    ) -> RecvBatch {
        let mut batch = RecvBatch::default();
        while batch.count < max_count {
            let entry = match self.pending.take() {
                Some(entry) => entry,
                None => match self.receiver.rx.try_recv() {
                    Ok(entry) => entry,
                    Err(_) => break,
                },
            };
            let size = entry.0 as usize;
            if size > max_bytes.saturating_sub(batch.bytes) {
                self.pending = Some(entry);
                batch.next_entry_bytes = Some(size);
                break;
            }

            buf.push(entry.1);
            batch.bytes += size;
            batch.count += 1;
        }
        // Batch return the permit returns here
        self.receiver.semaphore.add_permits(batch.bytes);
        batch
    }

    /// Same as `try_recv_many_with_buf` but waits until at least one element is available.
    pub async fn recv_many_with_buf(
        &mut self,
        buf: &mut Vec<T>,
        byte_limit: usize,
        mut max_count: usize,
    ) -> RecvBatch {
        if max_count == 0 {
            return RecvBatch::default();
        }

        if self.pending.is_none() {
            let Some(entry) = self.receiver.rx.recv().await else {
                return RecvBatch::default();
            };
            self.pending = Some(entry);
        }

        if byte_limit == usize::MAX && max_count == usize::MAX {
            max_count = self.len();
        }
        if byte_limit == usize::MAX {
            // If we have a max count set, and no byte limit, it's fair to
            // just reserve the vector capacity beforehand.
            buf.reserve(max_count.min(self.len()));
        }
        self.recv_many_internal(buf, max_count, byte_limit)
    }

    /// Attempts to receive as much elements into buffer for as long as:
    /// - The channel has elements to receive. It stops if receiving from the channel would block (or if the channel is closed).
    /// - Adding the next element would exceed `byte_limit`.
    /// - The number of received elements reaches `max_count`.
    /// - If both limits are [`usize::MAX`], this will drain all the elements that are available in the channel at the moment
    ///   of the call. Newly enqueued elements after the draining has started will not be included in the batch. This is a safety
    ///   mechanism to avoid blowing up the memory usage if there's an active sender.
    ///
    /// If the next element does not fit within `byte_limit`, its size is returned in
    /// [`RecvBatch::next_entry_bytes`] and the element remains available for a later receive.
    ///
    /// Note: This is more efficient than calling `try_recv` repeatedly as it releases all the permits in one go.
    pub fn try_recv_many_with_buf(
        &mut self,
        buf: &mut Vec<T>,
        byte_limit: usize,
        mut max_count: usize,
    ) -> RecvBatch {
        if max_count == 0 {
            return RecvBatch::default();
        }
        if byte_limit == usize::MAX && max_count == usize::MAX {
            max_count = self.len();
        }
        if byte_limit == usize::MAX {
            // If we have a max count set, and no byte limit, it's fair to
            // just reserve the vector capacity beforehand.
            buf.reserve(max_count.min(self.len()));
        }
        self.recv_many_internal(buf, max_count, byte_limit)
    }

    pub fn close(&mut self) {
        self.receiver.close();
    }

    pub fn is_closed(&self) -> bool {
        self.receiver.is_closed()
    }

    /// The number of messages currently buffered in the channel.
    pub fn len(&self) -> usize {
        self.receiver.len() + self.pending.is_some() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_none() && self.receiver.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    /// A message whose estimated size is just the wrapped byte count.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Sized(u32);

    impl EstimatedSize for Sized {
        fn estimated_size(&self) -> u32 {
            self.0
        }
    }

    #[tokio::test(start_paused = true)]
    async fn respects_byte_budget() {
        let (tx, mut rx) = channel::<Sized>(10);
        assert_eq!(rx.len(), 0);
        assert_eq!(tx.capacity(), 10);

        // Two 4-byte messages fit within the 10-byte budget.
        tx.send(Sized(4)).await.unwrap();
        assert_eq!(tx.capacity(), 6);
        tx.send(Sized(4)).await.unwrap();
        assert_eq!(rx.len(), 2);
        assert_eq!(tx.capacity(), 2);

        // A third message would exceed the budget (8 + 4 > 10), so the send
        // must block until capacity is freed.
        assert!(
            timeout(Duration::from_millis(50), tx.send(Sized(4)))
                .await
                .is_err()
        );

        // Draining one message frees 4 bytes, so the same send now succeeds.
        assert_eq!(rx.recv().await.unwrap().0, 4);
        assert_eq!(tx.capacity(), 6);

        tx.send(Sized(4)).await.unwrap();
        assert_eq!(rx.len(), 2);
        assert_eq!(tx.capacity(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn fifo_order() {
        let (tx, mut rx) = channel::<Sized>(10);

        tx.send(Sized(6)).await.unwrap();

        // Trying to send 5 bytes will block.
        let mut send_fut1 = std::pin::pin!(tx.send(Sized(5)));
        assert!(
            timeout(Duration::from_millis(50), send_fut1.as_mut())
                .await
                .is_err()
        );

        // Trying to acquire any other permit (even if it fits) will block because
        // there's a waiter waiting for a blocked permit.
        let mut send_fut2 = std::pin::pin!(tx.send(Sized(1)));
        assert!(
            timeout(Duration::from_millis(50), send_fut2.as_mut())
                .await
                .is_err()
        );
        // Similarly, a try reserve will fail.
        assert!(matches!(
            tx.try_reserve(1),
            Err(SendError {
                kind: SendErrorKind::InsufficientCapacity,
                ..
            })
        ));

        // Now drain the big message.
        assert_eq!(rx.recv().await.unwrap().0, 6);

        // Sends can now proceed.
        send_fut1.await.unwrap();
        send_fut2.await.unwrap();

        assert_eq!(rx.recv().await.unwrap().0, 5);
        assert_eq!(rx.recv().await.unwrap().0, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reserve_works() {
        let (tx, mut rx) = channel::<Sized>(10);

        let mut permit = tx.reserve(6).await.unwrap();
        assert_eq!(tx.capacity(), 4);

        // Sends more than the capacity blocks.
        assert!(
            timeout(Duration::from_millis(50), tx.send(Sized(5)))
                .await
                .is_err()
        );

        // Sends within the capacity should still succeed because reservations
        // guarantee capacity, not an ordering slot.
        tx.send(Sized(4)).await.unwrap();
        assert_eq!(tx.capacity(), 0);

        // Sending less than what was reserved, refunds the difference.
        // We reserved 6 bytes, but only sending 2. We should refund
        // the remaining 4 to the queue.
        permit.send(Sized(2)).unwrap();
        assert_eq!(tx.capacity(), 4);

        // Reserving and dropping the permit before sending returns the capcity.
        permit = tx.reserve(4).await.unwrap();
        assert_eq!(tx.capacity(), 0);
        drop(permit);
        assert_eq!(tx.capacity(), 4);

        // Reserve many
        let mut permits = tx.reserve_many(4).await.unwrap();
        assert_eq!(tx.capacity(), 0);
        permits.send(Sized(2)).unwrap();
        permits.send(Sized(1)).unwrap();
        // We had one byte left, dropping the permits should return it.
        drop(permits);
        assert_eq!(tx.capacity(), 1);

        // Now drain:
        assert_eq!(rx.recv().await.unwrap().0, 4);
        assert_eq!(rx.recv().await.unwrap().0, 2);
        assert_eq!(rx.recv().await.unwrap().0, 2);
        assert_eq!(rx.recv().await.unwrap().0, 1);
        assert_eq!(rx.len(), 0);
        assert_eq!(tx.capacity(), 10);
    }

    #[tokio::test(start_paused = true)]
    async fn reserve_errors() {
        let (tx, _rx) = channel::<Sized>(10);

        // Bigger than what this channel can ever hold, fails with SendErrorKind::EntryTooLarge
        assert!(matches!(
            tx.try_reserve(15),
            Err(SendError {
                kind: SendErrorKind::EntryTooLarge,
                ..
            })
        ));

        let permit = tx.reserve(6).await.unwrap();

        // Not enouch capacity for now
        assert!(matches!(
            tx.try_reserve(6),
            Err(SendError {
                kind: SendErrorKind::InsufficientCapacity,
                ..
            })
        ));

        // Sending more than what's reserved will fail, and the capacity
        // will be returned.
        assert!(matches!(
            permit.send(Sized(7)),
            Err(SendError {
                kind: SendErrorKind::PermitEntryTooLarge,
                ..
            })
        ));
        assert_eq!(tx.capacity(), 10);

        // Reserve many
        let mut permits = tx.reserve_many(7).await.unwrap();

        // Sending more than what's reserved will fail, but the reserved
        // capacity will remain.
        assert!(matches!(
            permits.send(Sized(9)),
            Err(SendError {
                kind: SendErrorKind::PermitEntryTooLarge,
                ..
            })
        ));
        assert_eq!(permits.capacity(), 7);

        // After consuming some permits, capacity drops.
        permits.send(Sized(4)).unwrap();
        assert_eq!(permits.capacity(), 3);
        assert!(matches!(
            permits.send(Sized(4)),
            Err(SendError {
                kind: SendErrorKind::PermitEntryTooLarge,
                ..
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn try_recv_many_with_buf() {
        let prep_full_channel = async || {
            let (tx, rx) = channel::<Sized>(50);

            // Enqueue 10 5-byte messages
            for _ in 0..10 {
                tx.send(Sized(5)).await.unwrap();
            }
            (tx, rx)
        };

        // Count only limit
        {
            let (_tx, rx) = prep_full_channel().await;
            let mut rx = BatchReceiver::new(rx);
            let mut buf = Vec::new();
            let batch = rx.try_recv_many_with_buf(&mut buf, usize::MAX, 3);
            assert_eq!(batch.count, 3);
            assert_eq!(batch.bytes, 15);
            assert_eq!(batch.next_entry_bytes, None);

            assert_eq!(buf, vec![Sized(5); 3]);
        }

        // Byte only limit
        {
            let (tx, rx) = prep_full_channel().await;
            let mut rx = BatchReceiver::new(rx);
            let mut buf = Vec::new();
            let batch = rx.try_recv_many_with_buf(&mut buf, 4, usize::MAX);
            assert_eq!(batch.count, 0);
            assert_eq!(batch.bytes, 0);
            assert_eq!(batch.next_entry_bytes, Some(5));
            assert_eq!(rx.len(), 10);
            assert_eq!(tx.capacity(), 0);

            let batch = rx.try_recv_many_with_buf(&mut buf, 7, usize::MAX);
            assert_eq!(batch.count, 1);
            assert_eq!(batch.bytes, 5);
            assert_eq!(batch.next_entry_bytes, Some(5));
            assert_eq!(buf, vec![Sized(5)]);
            let batch = rx.try_recv_many_with_buf(&mut buf, usize::MAX, 1);
            assert_eq!(batch.count, 1);
            assert_eq!(buf, vec![Sized(5); 2]);
            assert_eq!(tx.capacity(), 10);
        }

        // Byte and count limit
        {
            let (_tx, rx) = prep_full_channel().await;
            let mut rx = BatchReceiver::new(rx);
            let mut buf = Vec::new();
            let batch = rx.try_recv_many_with_buf(&mut buf, 13, 4);
            assert_eq!(batch.count, 2);
            assert_eq!(batch.bytes, 10);
            assert_eq!(batch.next_entry_bytes, Some(5));
            assert_eq!(buf, vec![Sized(5); 2]);
        }

        // No limit (drain)
        {
            let (_tx, rx) = prep_full_channel().await;
            let mut rx = BatchReceiver::new(rx);
            let mut buf = Vec::new();
            let batch = rx.try_recv_many_with_buf(&mut buf, usize::MAX, usize::MAX);
            assert_eq!(batch.count, 10);
            assert_eq!(batch.bytes, 50);
            assert_eq!(batch.next_entry_bytes, None);
            assert_eq!(buf, vec![Sized(5); 10]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn closing_works() {
        let (tx, mut rx) = channel::<Sized>(10);
        tx.send(Sized(3)).await.unwrap();
        tx.send(Sized(4)).await.unwrap();

        let tx1 = tx.clone();
        let tx2 = tx.clone();
        let tx3 = tx.clone();

        let j1 = tokio::spawn(async move {
            // Will block
            tx1.send(Sized(4)).await
        });
        let j2 = tokio::spawn(async move {
            // Will block
            let _p = tx2.reserve(4).await?;
            Ok(())
        });
        let j3 = tokio::spawn(async move {
            // Will block until closed
            tx3.closed().await
        });

        rx.close();
        assert!(rx.is_closed());

        assert!(matches!(
            j1.await.unwrap(),
            Err(SendError {
                kind: SendErrorKind::Closed,
                ..
            })
        ));
        assert!(matches!(
            j2.await.unwrap(),
            Err(SendError {
                kind: SendErrorKind::Closed,
                ..
            })
        ));
        assert_eq!(j3.await.unwrap(), ());
    }
}
