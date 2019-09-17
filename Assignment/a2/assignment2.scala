//Written by Gavin
//Student ID: z5195349  Student Name: Wenxun Peng
// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath  = "myfile.txt"
val outputDirPath = "outputFolder"


// Write your solution here
//Load our input data.
val file = sc.textFile(inputFilePath)

//Split up into lines, every line has one log of http
val word = file.flatMap(line => line.split("\n"))

//This function is to remove the last chars: "B", "KB" and "MB"
def Convert_bytes(str:String): String = {
	var res = ""

	//Using to transfor the "MB" and "KB" to "B"
	var coeff = 1
	var number = ""

	// remove the last char -> "B"
	res = str.substring(0, str.length()-1)

	// if the "MB" or "KB" exists
	var word = res.substring(res.length-1)
	res = res.substring(0, res.length()-1)
	if (word == "M")
		coeff = 1024*1024
	else if (word == "K")
		coeff = 1024

	// it is just the number
	else
		number = word
	
	word = res.substring(res.length-1)
	res = res.substring(0, res.length()-1)

	//Scan characters(numbers) from the end of the line and
	//get all numbers until find the " " or ","
	while(word != "," && word != " "){
		number = word+number
		word = res.substring(res.length-1)
		res = res.substring(0, res.length()-1)
	}

	//Transfer the number ("MB" or "KB" -> "B")
	number = (number.toInt * coeff).toString

	//Get the ",", add the "," before the numbers
	if (word == ",")
		res = res + "," + number
	return res
}

// Map and use above function to get the changed_bytes
val changed_bytes = word.map(x => Convert_bytes(x))

// Split up by ",", and get the line(0) (which is the URL, like: https://...)
// and the line(3) (which is the size of payload, like 3B, 431MB ...),
// and then map them as a new RDD (generating the (key, value) pairs)
val pairs = changed_bytes.map(word => word.split(",")).map(line => (line(0),line(3)))
//val ff = pairs.map(line => (line(0),line(3)))

//Combine the same key
val count = pairs.groupByKey()
//
//var values_count = count.take(count.count.toInt)

//Using this function to calculate the max, min, mean and variance value
//and transforing them to "XXXXXB" (such as 3B, 129024B)
def output_function(list:List[String]) : String = {

	// Represent the positive infinity, bigger than this value may overflow
	var min = 9223372036854775807L

	var max = 0L
	var sum = 0L
	var count = 0
	var mean = 0L
	var variance = 0L
	var result = ""
	
	//Get the max and the min
	for (x <- list){
		if (x.toLong > max)
			max = x.toLong
		if (x.toLong < min)
			min = x.toLong
		sum = sum + x.toLong
		count += 1
	}

	//Calculate the mean value
	mean = sum/count

	//Calculate the variance value
	for (x <- list){
		variance = variance + ((x.toLong - mean) * (x.toLong - mean))
	}
	variance = variance/count

	//Return the "XXXXB"
	result = min.toString + "B," + max.toString + "B," + mean.toString + "B," + variance.toString + "B"
	return result
}

//Using key combines with "," and the above function to get the final values
val output = count.map(v => (v._1.union(",").union(output_function(v._2.toList))))
//Output
output.coalesce(1).saveAsTextFile(outputDirPath)



