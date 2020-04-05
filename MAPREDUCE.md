# Lab1 实现一个MapReduce框架
## 0.运行一个顺序执行的 **mapreduce** 例子体验一下程序逻辑
 ```bash
cd src/main
// 将用户自定义的函数编译为动态连接库来执行
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrsequential.go wc.so pg*.txt
more mr-out-0
 ```
## 1. 如何启动一个分布式的 **mapreduce**？
### 启动一个 master
```bash
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrmaster.go pg-*.txt
```
###  启动多个 worker
打开多个终端 进入到项目的`main`下执行`go run mrworker.go wc.so`
     
## 2. 完成实验
###  编码
1. 在 `src/mr` 目录下完成实验内容
2. `mrmaster.go`会调用你编写的 `src/mr/master.go`代码
3. `mrworker.go`会调用`src/mr/worker.go`的代码
4. 二者通信的代码在`src/mr/rpc.go`中自行实现
5. 我已经经完成了实验代码,在`src/mroriginal`目录下有原始的实验框架是我留给你来独立完成的
6. 将mr文件夹进行覆盖即可:`mv src/mroriginal src/mr` 或者备份下留作参考😁

**ps:在`src/mrapps`目录下的是**MR**相关的应用函数**
 
### 调试
通过与`cat mr-out-* | sort` 输出的结果来对比可以检查你编写的MR框架是否运行正确

### 测试
在 `src/main`目录下执行 `sh test-mr.sh`由于master程序默认永远不退出,脚本执行后注意 `killall mr*` 释放掉资源.

## 更多Lab1实验的内容
[请点击这里](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html)

    