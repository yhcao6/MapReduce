# rm mydedup.index
rm -rf local/*

javac -cp .:lib/* LocalStorage.java Index.java MyDedup.java && 

# java -cp .:./lib/* MyDedup upload 400 512 1000 10 ./1.txt local
# java -cp .:./lib/* MyDedup upload 400 512 1000 257 ./1.png local
java -cp .:./lib/* MyDedup upload 4 13 1000 10 ./1.txt local

rm *.class
