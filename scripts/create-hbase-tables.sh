#!/bin/sh

./bin/start-hbase.sh

tableSummaries='summaries'
tableMood='mood'
tableMinMood='minmood'
tableCategories='categories'

#Table summaries
echo "exists '$tableSummaries'" | ./bin/hbase shell > log
cat log | grep "Table $tableSummaries does exist"
if [ $? = 0 ];then
    echo "************  table $tableSummaries already exists **********"

    echo "disable '$tableSummaries'" | ./bin/hbase shell
    echo "drop '$tableSummaries'" | ./bin/hbase shell
    echo "create '$tableSummaries','data'" | ./bin/hbase shell
else
    echo "***********  Need to create the table $tableSummaries  **********"
	echo "create '$tableSummaries', 'data' " | ./bin/hbase shell
fi
	
#Table mood
echo "exists '$tableMood'" | ./bin/hbase shell > log
cat log | grep "Table $tableMood does exist"
if [ $? = 0 ];then
    echo "************  table $tableMood already exists **********"

    echo "disable '$tableMood'" | ./bin/hbase shell
    echo "drop '$tableMood'" | ./bin/hbase shell
    echo "create '$tableMood','data'" | ./bin/hbase shell
else
    echo "***********  Need to create the table $tableMood  **********"
	echo "create '$tableMood', 'data' " | ./bin/hbase shell
fi

#Table minmood
echo "exists '$tableMinMood'" | ./bin/hbase shell > log
cat log | grep "Table $tableMinMood does exist"
if [ $? = 0 ];then
    echo "************  table $tableMinMood already exists **********"

    echo "disable '$tableMinMood'" | ./bin/hbase shell
    echo "drop '$tableMinMood'" | ./bin/hbase shell
    echo "create '$tableMinMood','data'" | ./bin/hbase shell
else
    echo "***********  Need to create the table $tableMinMood **********"
	echo "create '$tableMinMood', 'data' " | ./bin/hbase shell
fi

#Table minmood
echo "exists '$tableCategories'" | ./bin/hbase shell > log
cat log | grep "Table $tableCategories does exist"
if [ $? = 0 ];then
    echo "************  table $tableCategories already exists **********"

    echo "disable '$tableCategories'" | ./bin/hbase shell
    echo "drop '$tableCategories'" | ./bin/hbase shell
    echo "create '$tableCategories','data'" | ./bin/hbase shell
else
    echo "***********  Need to create the table $tableCategories **********"
	echo "create '$tableCategories', 'data' " | ./bin/hbase shell
fi
	
	



