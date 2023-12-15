#!/usr/bin/sh

echo "What is the initial name?"
read original_name

echo "What is the new name?"
read new_name

if [[ $new_name == *"-"* ]]
then
  echo "New project name should not contain any '-'. Replace '-' by '_'"
  exit 1
fi

if [ ${#new_name} -lt 4 ]
then
  echo "New project name '$new_name' is too short"
  exit 1
fi

echo "Renaming everything"
echo "Changing from '$original_name' to '$new_name'"


rename_in_file() {
  sed -i "s/$2/$3/g" $1
}

rename_in_folder() {
  for file in $(find $1 -type f)
  do
    rename_in_file $file $2 $3
  done
}


# Renaming the source code itself
rename_in_folder $original_name $original_name $new_name
mv $original_name $new_name
rename_in_folder "test" $original_name $new_name
rename_in_folder "notebook" $original_name $new_name

# Renaming in different folders
rename_in_folder script/ $original_name $new_name
#rename_in_folder doc/source/  $original_name $new_name

# Renaming in specific files
rename_in_file "MANIFEST.in" $original_name $new_name
