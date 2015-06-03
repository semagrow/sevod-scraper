head -n 3 $1
for i in $*
do
	tail -n +4 $i
done
