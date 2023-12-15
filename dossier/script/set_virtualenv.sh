##############################################################################
# 1. LOCATE VIRTUALENV BINARY
# 2. CREATE VIRTUALENV with the name passed in parameter
##############################################################################
echo "****************************************************"
echo "**           SET VIRTUAL ENVIRONMENT              **"
echo "****************************************************"
export SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
echo ""
echo "Loading set_virtualenv.sh from this repository: $SCRIPT"

VIRTENV_NAME=$1
# command to find VIRTUALENV BINARY
sentence=`whereis virtualenv`
venvbin="virtualenv"
v=0
v2=0
v3=0

for word in $sentence
do
# confirm that the file really exists
  if [ -z "${word##*virtualenv}" ]
  then
        v=1
# check to find if it virtualenv from python2 or 3
# take in priority the version 3
        if [ -z "${word##*2*}" ] && [ $v3 -eq 0 ]
        then
            v2=1
            venvbin=$word
        fi
        if [ -z "${word##*3*}" ] && [ ! -z "${word##*3.9*}" ]
        then
            v3=1
            venvbin=$word
        fi
        if [ $v2 -eq 0 ] && [ $v3 -eq 0 ]
        then
            venvbin=$word
        fi
    fi
done

virtalenvHelp=`$venvbin --h | wc -l`

# Check if the Virtualenv selected above really exists by checking its help page
if [ "$virtalenvHelp" -gt 2 ]
then
    echo "Path Virtualenv: $venvbin ==> OK($virtalenvHelp)"
    export VIRTUALENV_BIN=$venvbin
# Check if it's a Virtual environment already exists before creating it
    if [ ! -d "$VIRTENV_NAME" ]
    then
        echo "Create Virtualenv:  $VIRTENV_NAME"
# Check if it's a Ubuntu system because it's not creating a python 3 environment by default and it doesn't take the same parameter to create it
        if [ `uname -a | grep Ubuntu | wc -l` -eq 1 ]
        then
            $VIRTUALENV_BIN -p python3 $VIRTENV_NAME
        else
            $VIRTUALENV_BIN $VIRTENV_NAME
        fi
    else
        echo "INFO : Virtualenv all ready exists do not create it :  $VIRTENV_NAME"
    fi
else
    echo "Path Virtualenv: $venvbin ==> FAILED($virtalenvHelp)"
    exit 1
fi
echo
