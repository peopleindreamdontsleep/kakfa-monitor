# kafka-monitor
原kafka monitor是由sbt编写成的，国内下载和改写非常的不方便，在生产环境时也出现了kafka group会消失的bug，经过长时间的排查之后发现是kafka版本的问题，最新的kafka monitor是基于0.8的，官网说明0.10之后就解决了这个bug，在将kakfa依赖改变的时候，大部分的zkclient的方法也变化了，将原来的代码经过了一些重构。
## 总的变化
1.改变前端一个js的下载地址，使得不用翻墙也能在前端查看效果。

2.将原来的sbt编写编程maven，更加通用。

3.将kafka版本编程0.10，解决了group消失的bug，并重写了相关获取方法。
