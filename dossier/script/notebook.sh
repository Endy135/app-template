#!/bin/sh
# usages:
# (venv) $ ./script/notebook start
# (venv) $ ./script/notebook stop

# set -e


# Directories to use (relative to project path)
JUPYTERLOG=$UNXLOG/jupyter
if [ ! -d "$JUPYTERLOG" ]; then
mkdir -p $JUPYTERLOG
fi
if [ ! -d "$UNXNOTEBOOK" ]; then
mkdir -p $UNXNOTEBOOK
fi

# STEP 1: locate existing jupyter processes

# STEP 1.1: read saved url from previous run
saved_url=$JUPYTERLOG/saved_url.txt
touch $saved_url
urls=$(cat $saved_url)

# STEP 1.2: locate jupyter notebooks on this machine and create matching urls
jupyter_pids=$(ps aux | grep $USER | grep 'jupyter-notebook' | grep -v grep |  awk '{print $2}')
for pid in $jupyter_pids
do
  ports_used_by_pid=$(ss -lpn | grep ",$pid," | awk '{print $3}' | awk -F ':' '{print $NF}' | sort | uniq)
  for port in $ports_used_by_pid
  do
    if [[ $HOSTNAME == *"inetpsa.com" ]]; then
      url="http://$HOSTNAME:$port"
    elif [[ $HOSTNAME == *"inetpsa" ]]; then
      url="http://$HOSTNAME.com:$port"
    else
      url="http://$HOSTNAME.inetpsa.com:$port"
    fi
    urls=$urls$'\n'$url
  done
done
# number of urls (removing blank lines)
n_urls=$(echo  "$urls" | sed '/^\s*$/d' | wc -l)


stop() {
  if [ -n "$jupyter_pids" ]
  then
    echo "Killing jupyter processes on this machine ($HOSTNAME)"
    for pid in $jupyter_pids
    do
      echo "killing $pid"
      kill $pid
    done
  fi
}

search_available_port () {
	for ((o=10000; o<=10100; o++))
	do
		eval netstat -an | grep $o > /dev/null
    ret_code=$?
		if [ $ret_code -eq 1 ]
		then
			echo $o
			break
		fi
	done
}

launch() {
  echo "Launching notebook"
  port=$(search_available_port)
  if [ ! -d "$JUPYTERLOG" ]; then
  mkdir -p $JUPYTERLOG
  fi
  if [ ! -d "$UNXNOTEBOOK" ]; then
  mkdir -p $UNXNOTEBOOK
  fi
  jupyter notebook password --log-level=ERROR
  nohup jupyter notebook \
    --ip='0.0.0.0' \
    --port=$port \
    --notebook-dir=$UNXNOTEBOOK \
    --log-level=CRITICAL \
    > $JUPYTERLOG/jupyter_app.log 2> $JUPYTERLOG/jupyter_err.log &
  if [[ $HOSTNAME == *"inetpsa.com" ]]; then
    url="http://$HOSTNAME:$port"
  elif [[ $HOSTNAME == *"inetpsa" ]]; then
    url="http://$HOSTNAME.com:$port"
  else
    url="http://$HOSTNAME.inetpsa.com:$port"
  fi
  echo $url > $saved_url
  echo "Sending logs to $jupyter_logs"
  echo "Started notebook, access it with:"
  echo ""
  echo $url
  echo ""
}

remove_logs() {
  if [ -f $JUPYTERLOG/jupyter_app.log ]
	then
	   rm $JUPYTERLOG/jupyter_app.log
	fi

	if [ -f $JUPYTERLOG/jupyter_err.log ]
	then
	   rm $JUPYTERLOG/jupyter_err.log
	fi
}

# FUNCTION START:
# show existing notebooks and allow user to close them
start() {
  if [ $n_urls -gt 0 ]
  then
    echo "You probably have existing jupyter started ($n_urls)."
    echo "Try using one of the following urls:"
    echo "$urls"
    echo ""
    read -n 1 -p "Start a new jupyter process and kill existing ones? [y/n] " choice
    echo ""
    if [[ "$choice" =~ [y] ]]
    then
      stop
      remove_logs
      launch
    else
      echo "Keeping existing notebooks"
    fi
  else
    echo "Starting a new jupyter notebook"
    remove_logs
    launch
  fi
}


# MAIN SCRIPT
trap "exit" INT
if [[ "$1" == "start" ]]
then
  start
else
  if [[ "$1" == "stop" ]]
  then
    stop
  else
    echo "Wrong usage. Correct usages are:"
    echo "(venv-projet) $ ./script/notebook start"
    echo "(venv-projet) $ ./script/notebook stop"
    exit 1
  fi
fi
