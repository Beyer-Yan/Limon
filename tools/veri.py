
start = 1032192
i = 73728
res = []
while i < 128*1024:
    if start % i ==0:
        res.append(i)
    i = i+1


#print(res)

for i in range(15):
    k = 0
    for j in range(4):
        if( (~i)&(1<<j)!=0 ):
            k = j
            break
    print(i,bin(i),k)