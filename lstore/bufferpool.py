class BufferPool:
    """The BufferPool manages storing pages in cache
    """

    def __init__(self, block_size, cache_size):
        """
        block_size - number of pages stored in one block
        cache_size - max number of blocks stored in-memory
        """
        self.block_size = block_size
        self.cache_size = cache_size


    def append_page(self, page_id):
        pass
    
    
    def get_page(self, page_id):
        pass
        
    def update_page(self, page_id):
        pass
        