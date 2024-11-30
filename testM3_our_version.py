"""
Consistency challenges
updating a primary key to an existing primary key should fail. Even while a bunch of other stuff is going on
pk1 updating to pk4. meanwhile pk2 to pk1
updating on something that doesn't exist yet.
serializability
Transaction: a list of queries. It should be isolated and either commit or abort (and then try again)
T1 inserts n tuples. T2 updates n tuples. Make T1 abort 


class LOCK MANAGER()

    def init(self)

    def request_lock(resource_id, exculsive?: bool) -> bool (probably)

    def release_lock(resource_id)

    def _construct_list_of_locks_that_exist()
        each tuple is associated with a rid. Just make a lock for each rid


how are we going to handle versions?
"""