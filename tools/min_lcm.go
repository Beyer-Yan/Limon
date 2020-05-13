package main

import "math"
import "fmt"
import "sync"
import "os"
import "strconv"

var gOpt = 4*1024*1024

const(
	MAX_SLOT = 76*1024
	BS = 4*1024
)

var E1 = 4300
var E2 = 72*1024

var wg sync.WaitGroup
var m  sync.Mutex

type searchNode struct {
	path []int
	curLcm int
}

func lcm(iptA int, iptB int) int{
	a := iptA
	b := iptB
	c := 0
    for {
		if (b != 0){
			c = a % b
			a = b
			b = c
		}else{
			break
		}
	} 
	// then calc the lcm
	return  int(iptA * iptB / a)
}

func gcd(iptA int, iptB int) int{
	tmp := 0
	if iptA < iptB{
        tmp = iptA 
        iptB = iptA
        iptA = tmp
	}
 
	for{
		if (iptB > 0){
			tmp = iptA % iptB
			iptA = iptB
			iptB = tmp
		}else{
			break
		}
	}
    return iptA
}
/*
func bound(val int, curLcm int) int{
	return lcm(val,curLcm)
}
*/

func calcMaxVacancy(node *searchNode){

}
func treaceSolution(node *searchNode) {
	m.Lock()
	fmt.Printf("depth:%d, lcm:%d, max:%d, --> ",len(node.path),node.curLcm)
	fmt.Print("[ ")
	for _, val := range(node.path){
		fmt.Print(val," ")
	}
	fmt.Println("]")
	m.Unlock()
}

func checkValidity(curLcm int,val int,C2 int, C3 int) bool{
	isValid := false
	bd := lcm(curLcm,val)
	if (bd<0){
		return false
	}

	if (lcm(val,BS)<C2){
		if (val%BS ==0){
			isValid = true
		}else{
			ri := val%BS
			gi := BS%ri
			minGap := gcd(ri,gi)
			if (minGap>=C3 && bd < gOpt){
				isValid = true
			}
		}
	}
	return isValid
}

func findSolution(node *searchNode, C1 float64,C2 int, C3 int) {
	pathLen := len(node.path)
	curNode := &searchNode{
		path : make([]int,pathLen+1),
	}
	copy(curNode.path,node.path)

	val := node.path[pathLen-1]
	maxVal := int(math.Floor(float64(val)*(1+C1)))
	curLcm := node.curLcm

	for{
		if (maxVal>val){
			if (checkValidity(curLcm,maxVal,C2,C3)){
				curNode.path[pathLen] = maxVal
				curNode.curLcm = lcm(node.curLcm, maxVal)

				// Now I terminate
				if (maxVal>=E2){
					if(curNode.curLcm < gOpt){
						gOpt =curNode.curLcm
						treaceSolution(curNode)
					}
				}else{
					findSolution(curNode,C1,C2,C3)
				}
			}
			maxVal--
		}else{
			break
		}
	}
}

func start(initNode *searchNode,C1 float64, C2 int, C3 int){
	findSolution(initNode,C1,C2,C3)
	fmt.Printf("search path completes:%d\n",initNode.path[0])
	wg.Done()
}

func main() {
	//C1 := 0.05
	//C2 := 4*1024*1024
	//C3 := 32

	C1,_ := strconv.ParseFloat(os.Args[1],64)
	C2,_:= strconv.Atoi(os.Args[2])
	C3,_ := strconv.Atoi(os.Args[3])

	initVal,_ := strconv.Atoi(os.Args[4]])
	endVal,_ := strconv.Atoi(os.Args[5])
	
	E1 = initVal
	E2 = endVal

	for val:=E1; val>BS; val-- {
		if(checkValidity(BS,val,C2,C3)){
			//fmt.Printf("valid init val:%d\n",val)
			node := &searchNode{
				path : make([]int, 1),
				curLcm : lcm(val,BS),
			}
			node.path[0] = val
			wg.Add(1)
			go start(node,C1,C2,C3)
		}
	}
	wg.Wait()
	fmt.Println("searching completes")
}