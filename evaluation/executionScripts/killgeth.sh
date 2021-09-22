killall geth

while pgrep geth > /dev/null; do
  echo Still running...
  sleep 1
done

