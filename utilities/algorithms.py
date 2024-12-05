# Assumes that list is sorted.
# If target isn't found, this returns the index that it would be inserted into.
# If there are duplicate values == target in the lst, return the first one.
def binary_search(lst: list, target):
    left, right = 0, len(lst) - 1
    result = -1 
    
    while left <= right:
        mid = (left + right) // 2
        if lst[mid] < target:
            left = mid + 1
        elif lst[mid] > target:
            right = mid - 1
        else:
            result = mid  
            right = mid - 1

    return result if result != -1 else left 

# Same assumptions and output of binary search.
# For a small len(lst), this may be faster due to cache locality and branch prediction.
def linear_search(lst: list, target):
    i = 0
    while i < len(lst) and lst[i] < target:
        i += 1
    return i