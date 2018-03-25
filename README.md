# MapReduce-for-Join

Code Logic:

1. Main Driver Program:
Initially we need to create jon object and set configurations. This step includes setting map and reduce classes,path to input,output files and type of key

2. Mapping logic:
Map function gets values in Text format which we can convert to string and extract key (second value after using spliting string). This key and string representation of record is used to make map of key,value pairs. Key is join column name and value is actual record.


3. Reducer of EquiJOin:
Reducer gets data produced from mapper. These records are stored in set to avoid duplicates. By checking the value of records we can maintain 2 sets with values from 2 tables.
After storing data in 2 sets we can perform join operation using 2 nested for loops and store the data in output collector using collect method by converting string value of join records into Text format.
