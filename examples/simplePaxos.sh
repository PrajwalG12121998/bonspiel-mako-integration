rm a1.log a2.log a3.log a4.log
nohup ./build/simplePaxos localhost > a1.log 2>&1 &
sleep 1 
nohup ./build/simplePaxos p1 > a2.log 2>&1 &
sleep 1
nohup ./build/simplePaxos p2 > a3.log 2>&1 &
sleep 1
nohup ./build/simplePaxos learner > a4.log 2>&1 &

sleep 10 

tail -n 1 a1.log a2.log a3.log a4.log

