class Node():
    def __init__(self, val, key):
        self.val = val     
        self.key = key     
        self.next = None
        self.prev = None
    
class LRUCache:
    def __init__(self, size):
        self.size = size
        self.map = {}
        self.dummy = Node(0,0)
        self.hd = self.dummy.next
        self.tl = self.dummy.next
             
    def remove_lru(self):
        if not self.hd:
            return
        prev = self.hd
        self.hd = self.hd.next
        if self.hd:
            self.hd.prev = None
        del prev
        
    def add_node(self, node):
        if not self.tl:
            self.hd = self.tl = node
        else:
            self.tl.next = node
            node.prev = self.tl
            self.tl = self.tl.next
        
    def rm_node(self, node):
        if self.hd is node:
            self.hd = node.next
            if node.next:
                node.next.prev = None
            return

        prev = node.prev
        nex = node.next
        prev.next = nex    
        nex.prev = prev
        
    def get(self, key):
        if key not in self.map:
            return -1
        
        node = self.map[key]
        if node is not self.tl:
            self.rm_node(node)
            self.add_node(node)

        return node.val
    
    def put(self, key, value):
        if key in self.map:
            self.map[key].val = value
            self.get(key)
            return
        
        if len(self.map) == self.size:
            self.map.pop(self.hd.key)
            self.remove_lru()
        
        new_node = Node(val=value, key=key)
        self.map[key] = new_node
        self.add_node(new_node)
        