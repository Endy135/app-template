#!/usr/bin/sh

####################################################
# USERNAME #
####################################################
echo "RPI id (lowercase is needed for first letter) : "
read username

if [[ $username == *['!'@#\$%^\&*()_+]* ]]
then
  echo "RPI id should not contain any special character '#@&+', please run the command again"
fi

if [ ${#username} -lt 7 ]
then
  echo "RPI id '$username' is too short, please run the command again"
fi

if [ ${#username} -gt 7 ]
then
  echo "RPI id '$username' is too long, please run the command again"
fi

####################################################
# PASSWORD #
####################################################
unset password;
echo "RPI password : "
password=''
while IFS= read -r -s -n1 char; do
  [[ -z $char ]] && { printf '\n'; break; } # ENTER pressed; output \n and break.
  if [[ $char == $'\x7f' ]]; then # backspace was pressed
      # Remove last char from output variable.
      [[ -n $password ]] && password=${password%?}
      # Erase '*' to the left.
      printf '\b \b'
  else
    # Add typed char to output variable.
    password+=$char
    # Print '*' in its stead.
    printf '*'
  fi
done

if [[ $password == *['!'@#\$%^\&*()_+]* ]]
then
  echo "RPI password should not contain any special character, please run the command again"
fi

if [ ${#password} -lt 8 ]
then
  echo "RPI password is too short, please run the command again"
fi

if [ ${#password} -gt 8 ]
then
  echo "RPI password is too long, please run the command again"
fi

####################################################
# PROXY #
####################################################
export HTTPS_PROXY="http://$username:$password@http.internetpsa.inetpsa.com:80"
export HTTP_PROXY="http://$username:$password@http.internetpsa.inetpsa.com:80"

echo "========= Proxy set for Azure ========="
