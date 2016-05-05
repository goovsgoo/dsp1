/********************************/
README!
/********************************/

DSP 162 assignment_1

Or Koren 201031226
Yoed Grizim 301675443

5/5/2016

###############################################

The purpose of this mission was to experiment with AWS.
In this assignment we wrote an application that take list of tweets and classifies and tags them.
The results returned as an HTML page.

Setup:
0. Download all files from the submission page to a folder
1. Put a rootkey.csv file near LocalApp.jar. The rootkey.csv has to be in the following format:
	accessKey=<key>
	secretKey=<secret key>
2. Compress the csv to file name rootkey.zip with password: <Or's password>
2. Create a bucket with a name: <credentials key lowercase>mybucket. e.g: akiai3bmpkxyzm2gf4gamybucket
3. Upload to that bucket the files:
	Manager.jar
	Worker.jar
	dsp1_v1_lib.zip
	rootkey.zip - your root key locked with password
4. Unzip dsp1_v1_lib.zip
5. Run the program via one of the two options:
  	a. java -jar LocalApp.jar inputfile outputfile n
  	b. java -jar LocalApp.jar inputfile outputfile n terminate
   
	When:
	<inputfile> - path of input
	<outputfile> - path of output HTML
	<n> - <workers/urls>
	[terminate] - Optional: command to fully terminate the system.

Example:
java -jar LocalApp.jar input.txt output.html 10 terminate

##########################################################
AWS info and simulation time results

AMI: ami-08111162
Type: t2-small
Simulation: 320 tweets, 40 tweets per worker (8 workers)
Times:	
	Including establishing the nodes : 5 min
    	Not including: 23 second      	

##########################################################   
Local:

1. Uploads input file to s3.
2. Puts a message in SQS queue Local -> Manager, regarding file name in S3, Task ID and "n" number.
3. Checks if manager exists and has "running" state. If not, starts manager instance.
4. Waits for a message to receive in SQS queue Manager->Local. Once recevied:
	4.1 If the message has the different Task ID as generated in this local -> return it back and wait 5 sec.
	4.2 Otherwise, proceed to 5
5. Downloads the HTML file from S3 and save it as <outputfile>
6. If should terminate, it sends 'terminate' message to the manager.

Manager:

2. Open a new thread that goes to 10 (Find and reduce responses from the workers).
3. Wait for a message to receive from SQS queue Local->Manager.
	3.1 If the message is 'terminate' go to 5
	3.2 Otherwise, open a new thread that goes to 4
	3.3 go to 3 
4. (Thread 'ExecuteTask') Downloads the input file from S3
	4.1 Splits the the file to string of tweets, and send each to the Manager->Worker queue.
	4.2 Thread dies	
5. Wait until the Manager->Worker queue is empty
6. Send termination messages to all workers. Wait here until all workers are dead.
7. Sutdown all threads
8. Deletes all SQS queues, and S3 tmp directory
9. ** terminate **

10. (Thread 'ExecuteFindAndReduce') If manager is not terminating:
	10.1 Pull as many messages as possible from Worker->Manager queue. For each message:
	10.2 If messsage body is not "Failed" append the message into HTML file
	10.3 Delete the message from SQS
	10.4 If the message was that last of its task:
		10.4.1 Close the HTML with proper tags
		10.4.2 Upload the HTML to S3
		10.4.3 Sends a message to local
	10.5 Go to 10

Worker:

1. Receives message from Manager -> Worker queue.
2. If the message is termination message, it creates statistic file and uploads it to s3.

Cases that will fail and were not handled:

1. If a worker sends an answer to the manager and falls before deleting the message from the queue,
then another worker will take the same tweet and send the same response to the manager. A double row will be added to the output.
2. When opened several LocalApps At the same time they may open multi managers instantces,
   because the manager is still not in 'running' mode. In that case there will be two managers.
3. When the manager receives two tasks and there are no active workers, the manager will open for every task its number
   of workers, instead of maximum number of workers bewtween the two.
   