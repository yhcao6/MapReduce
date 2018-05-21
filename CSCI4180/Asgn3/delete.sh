# rm mydedup.index

javac -cp .:lib/* LocalStorage.java Index.java MyDedup.java && 

java -cp .:./lib/* MyDedup delete ./1.txt local

rm *.class
