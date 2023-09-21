for num in {1..10}
do
  nohup sh ~/rmq/benchmark/producer.sh -n 10.0.1.38:9876 -t BenchmarkTest2 -s 4096 >> smc.txt &
done