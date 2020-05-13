import math
# global vars
E1 = 4300
E2 = 72*1024
MAX_SLOT = 76*1024
BS = 4*1024
g_Opt = 100000

class search_node(object):
    def __init__(self):
        self.condinator = []
        self.prev = None
        self.count = 0

def lcm(ipt_a,ipt_b):
    # calc the gcd first
    a = ipt_a
    b = ipt_b
    while b != 0:
        c = a % b
        a = b
        b = c
    # then calc the lcm
    result = int(ipt_a * ipt_b / a)
    return result

def gcd(ipt_a,ipt_b):
    if ipt_a < ipt_b :
        tmp = ipt_a 
        ipt_b = ipt_a
        ipt_a = tmp
    
    while ipt_b > 0 :
        tmp = ipt_a % ipt_b
        ipt_a = ipt_b
        ipt_b = tmp
     
    return ipt_a

def bound(x,C1) :
    return math.floor(math.log(MAX_SLOT/x,1+C1))

def trace_solution(node):
    res = []
    while node != None:
        res.append(node.condinator[-1])
        node = node.prev
    
    print(len(res),":",res)

def validity_check(solved_cnt,val, C1,C2,C3):
    global g_Opt
    is_valid = False
    if  lcm(val,BS)<C2:
        if val%BS==0:
            is_valid = True
        else:
            ri = val%BS
            gi = BS%ri
            min_gap = gcd(ri,gi)

            if min_gap>C3 and bound(val,C1)+solved_cnt<g_Opt:
                is_valid = True

    return is_valid

def find_solution(node, C1,C2,C3):
    global g_Opt
    cur_node = search_node()
    cur_node.prev = node
    cur_node.count = node.count+1

    prev_val = node.condinator[-1]
    max_val = math.floor(prev_val*(1+C1))

    while max_val > prev_val:
        if validity_check(cur_node.count,max_val,C1,C2,C3):
            cur_node.condinator.append(max_val)
            if max_val>=E2:
                # now i terminate
                if cur_node.count < g_Opt:
                    g_Opt = cur_node.count
                    trace_solution(cur_node)
            else:
                find_solution(cur_node,C1,C2,C3)
                    
        max_val = max_val - 1

if __name__ == "__main__":

    init_node = search_node()
    init_node.condinator.append(4096)
    init_node.count = 1

    find_solution(init_node,0.05,2*1024*1024,64)
