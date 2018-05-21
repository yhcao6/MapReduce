# rm mydedup.index
# rm -rf local/*

# javac -cp .:lib/* AzureStorage.java Index.java MyDedup.java && 

java MyDedup upload 1 2 8 10 ./1.txt azure
# java MyDedup upload 1 2 8 10 ./1.png local

rm *.class
