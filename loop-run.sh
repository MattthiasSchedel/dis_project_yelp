lo  
#!/bin/bash

# Run the Python script for each value of p
for p in 50 100 150 200
do
  echo "Running script for p = $p"
  
  # Run the Python script 5 times for the current value of p
  for i in {1..5}
  do
    spark-submit  main4.py $p
  done
  
  echo "Finished running script for p = $p"
done