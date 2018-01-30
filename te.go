package main
import "fmt"
func main(){
	ma := make(map[string](string))
	if(ma["asd"]==""){
		fmt.Printf("is nil\n")
	}else {
		fmt.Printf("not nil\n:")
	}
}
