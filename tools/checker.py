
import math

BS = 4096

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

def check_valid(prev_val,val):

    # condition 1
    if (val - prev_val)/val > 0.05:
        return False
    # condition 2
    lcm_num = lcm(val,BS)
    if lcm_num > 4*1096*4096:
        return False

    # condition 3
    num_xi = int(lcm_num/val)
    for i in range(1,num_xi):
        if i*val%BS<32:
            return False

    return True

if  __name__ == "__main__":
    res =  [ 4096,4608,5376,6144,7168,8192,9216,10752,12288,14336,16384,18432,21504,24576,28672,32256,36864,43008,49152,57344,64512,73728]
    print(len(res))
    max_gap = 0
    l = []
    for i in range(2,len(res)):
        if check_valid(res[i],res[i-1]):
            print("val:",res[i],":pass,","lcm:",lcm(res[i],4096))
            l.append(lcm(res[i],4096))
        gap = (res[i] - res[i-1])/res[i]
        max_gap =  gap if gap > max_gap else max_gap
    print("max gap:",max_gap)
    l.append(lcm(73728,4096))
    #print(l)

    l = []
    i = 150
    while i>=2:
        l.append(math.floor(4096/i))
        i = i-1
        
    #print("------")
    #print(len(l),":",l)

    l = 1
    res =  [1024,923,831,748,674,607,547,493,444,400,360,324,292,263,237,214,193,174,157,142,128,116,105,95,86,78,71,64,58,53,48,44,40,36,33,30,27, 25,23,21,19]
    for i in [23,21,19]:
        l = lcm(l,i)

    l = []
    res = [ 1024,923,831,748,674,607,547,493,444,400,360,324,292,263,237,214,193,174,157,142,128,116,105,95,86,78,71,64,58,53,48,44,40,36,33,30,27,25,23,21,19]
    res.reverse()
    print([i * 4096 for i in res])