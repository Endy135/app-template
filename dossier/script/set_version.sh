echo "*************************************************************"
echo "**   RETRIEVE LAST TAG FROM GIT AND SET IT FOR PACKAGING   **"
echo "*************************************************************"
tag_found=`git tag | tail -1 | wc -l`

if [ $tag_found -eq 0 ]
then
  echo "   ==> There is no TAG on Git. Set the version to 1.0.0-snapshot by default."
  sed -i 's/snapshot/1.0.0-snapshot/' $UNXPACKAGE/__init__.py
else
  list_tag=`git tag`

  # remove character v or V from the tag name to be able to sort them
  git tag > list_tag.txt
  for element in `cat list_tag.txt`
  do
      echo "$element" | sed 's/[vV]//g' >> list_tag_updated.txt
  done

  last_tag=`sort list_tag_updated.txt | tail -1`

  echo "   ==> Set the version to $last_tag"
  sed -i 's/snapshot/'`echo $last_tag`'/' $UNXPACKAGE/__init__.py

  rm list_tag_updated.txt
  rm list_tag.txt
fi
