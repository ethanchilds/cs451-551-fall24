"""
This defines various cache policies which
affect the behavior of a PriorityQueue.
"""

# System imports
import random


class CachePolicy():
    """An abstract policy for cache behavior

    This represents a basic abstract class
    for cache behavior.
    """

    def __init__(self, queue):
        """Initialize a CachePolicy

        This initializes a CachePolicy object
        with a given queue to manage.

        Parameters
        ----------
        queue : PriorityQueue
            The given PriorityQueue to manage
        """

        self.queue = queue

    def on_push(self):
        """Perform an action on push

        This occurs every time that there is a
        push operation to the queue.
        """

        pass

    def update_priority(self, old_priority):
        """Calculate the new priority of an item

        This calculates the new priority of an item
        given its current priority.

        Parameters
        ----------
        old_priority : int
            The current priority of the item
        
        Returns
        -------
        new_priority : int
            The new updated priority
        """

        return old_priority


class LRUCachePolicy(CachePolicy):
    """Least Recently Used Cache Policy

    This defines a Least-Recently-Used (LRU)
    cache policy which prioritizes keeping
    an item in the cache if it was used recently.
    """

    def on_push(self):
        pass

    def update_priority(self, old_priority):
        return old_priority + 1


class MRUCachePolicy(CachePolicy):
    """Most Recently Used Cache Policy

    This defines a Most-Recently-Used (MRU)
    cache policy which prioritizes keeping
    an item in the cache if it wasn't used recently.
    """

    def on_push(self):
        pass

    def update_priority(self, old_priority):
        return old_priority - 1


class ZeroWeightCachePolicy(CachePolicy):
    """Zero Weight Cache Policy

    This defines a zero weighted cache policy
    which has no preference on elements.
    This means that the queue proceeds to discard
    the first element that was added to it with
    no modification of priority.
    """

    def on_push(self):
        pass

    def update_priority(self, old_priority):
        return old_priority


class LeakyBucketCachePolicy(CachePolicy):
    """Leaky Bucket Cache Policy

    This defines a leaky bucket policy where
    each element in the priority queue slowly
    loses priority over time until zero and
    updated elements receive the max bucket
    capacity.
    """

    def __init__(self, queue, bucket_capacity=10, bucket_increment=10):
        """Initialize a LeakyBucketCachePolicy

        This initializes a LeakyBucketCachePolicy
        with a fixed max capacity for each bucket.

        Parameters
        ----------
        queue : PriorityQueue
            The given PriorityQueue to manage
        bucket_capacity : int (default=10)
            The maximum capacity for a bucket
        bucket_increment : int (default=10)
            The increment amount
        """

        super().__init__(queue)
        self.bucket_capacity = bucket_capacity
        self.bucket_increment = bucket_increment

    def on_push(self):
        """Perform an action on push

        Every step that the queue gets updated,
        all elements decrement down to zero.
        """
        for k in self.queue.map:
            old_priority = self.queue.get(k)[0]
            self.queue.set_priority(k, max(old_priority - 1, 0))

    def update_priority(self, old_priority):
        return min(old_priority + self.bucket_increment, self.bucket_capacity)


class InverseLeakyBucketCachePolicy(CachePolicy):
    """Inverse Leaky Bucket Cache Policy

    This defines an inverse leaky bucket policy where
    each element in the priority queue slowly
    increases priority over time until zero and
    updated elements receive the negated max bucket
    capacity.
    """

    def __init__(self, queue, bucket_capacity=10, bucket_decrement=10):
        """Initialize an InverseLeakyBucketCachePolicy

        This initializes an InverseLeakyBucketCachePolicy
        with a fixed max capacity for each bucket.

        Parameters
        ----------
        queue : PriorityQueue
            The given PriorityQueue to manage
        bucket_capacity : int (default=10)
            The maximum capacity for a bucket
        bucket_decrement : int (default=10)
            The decrement amount
        """

        super().__init__(queue)
        self.bucket_capacity = bucket_capacity
        self.bucket_decrement = bucket_decrement

    def on_push(self):
        """Perform an action on push

        Every step that the queue gets updated,
        all elements increment up to zero.
        """
        for k in self.queue.map:
            old_priority = self.queue.get(k)[0]
            self.queue.set_priority(k, min(old_priority + 1, 0))

    def update_priority(self, old_priority):
        return max(old_priority - self.bucket_decrement, -self.bucket_capacity)


class StochasticCachePolicy(CachePolicy):
    """Stochastic Cache Policy

    This defines a Least-Recently-Used (LRU)
    cache policy which prioritizes keeping
    an item in the cache if it was used recently.
    """

    def __init__(self, queue, min_value=0, max_value=10):
        """Initialize an InverseLeakyBucketCachePolicy

        This initializes an InverseLeakyBucketCachePolicy
        with a fixed max capacity for each bucket.

        Parameters
        ----------
        queue : PriorityQueue
            The given PriorityQueue to manage
        min_value : int (default=0)
            The minimum random value (inclusive)
        max_value : int (default=10)
            The maximum random value (inclusive)
        """

        super().__init__(queue)
        self.min_value = min_value
        self.max_value = max_value

    def on_push(self):
        pass

    def update_priority(self, old_priority):
        return random.randint(self.min_value, self.max_value)
