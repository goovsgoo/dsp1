/********************************/
README!
/********************************/

DSP 162 assignment_1

Or Koren 201031226
Yoed Grizim 301675443

5.5.16

###############################################

The purpose of this mission was to experiment with AWS.
In this assignment we wrote an application that take list of tweeters and classifies and tags them.
After will display the result on a HTML page.

Setup:
1. Make sure that Manager.jar and Worker.jar dsp1_v1_lib.zip and rootkey.zip files uploaded to s3 
    to bucket named "akiai3bmpkxyzm2gf4gamybucket".  
2. You have two ways to run the program:
  a. java -jar LocalApp.jar inputfile outputfile n
  b. java -jar LocalApp.jar inputfile outputfile n terminate
  If you want to terminate manager at the end of work, you should use the second option.
3. The program was tested to work with the version: aws-java-sdk-1.10.69.jar
   
When:
<inputfile> - path of input
<outputfile> - path of output HTML
<n> - <workers/urls>
[terminate] - optional command to fully terminate the system.

Example:
java -jar LocalApp.jar input.txt output.html 10 terminate

##########################################################
   
Local:
1. Looks for 2 queues: Local -> Manager and Manager -> Local.
2. Uploads input file to s3.
3. Checks if manager exists and has "running" or "pending" state. If it is not, is starts manager instance.
4. Sends a message to the manager: Input file name , Local ID  and "n" number.
5. downloads summary file from s3 and creates HTML file named <outputfile>
7. If terminate message was send, it sends Terminate message to the manager.

Manager:
1. Looks for 4 queues: Local -> Manager and Manager -> Local and  Worker -> Manager and Manager -> Worker.
3. Receives messages from Local -> Manager queue until it receives "Terminate" message.
4. Parses the message.
5. Runs new thread for each job request from Local.
6. If manager receives termination message:	
	* Receives all messages from locals that remained and deletes Local -> Manager queue.
	* Sends termination message to Manager -> Worker queue.
	* Waits until there are no more workers(number of instances is 1).
	* Deletes Worker -> Manager and Manager -> Worker queues.
	* Terminates workers
	* Deletes Manager -> Local queue
	* Terminates itself

Each thread in manager: 
1. main: wait for tasks from the local. when recived deliver task to thread ExecuteTask.
2. ExecuteTask: downloads the task from S3, distbrutes its to tweets, push the tweets to the workers.
3. ExecuteFindAndReduce: waits for Msgs from the workers, unifies them, by task, to an HTML page. when all msges of a task recived uploads the HTML to S3.

Worker:
1. Receives message from Manager -> Worker queue.
2. If the message is termination message, it creates statistic file and uploads it to s3.

Extreme cases that were not handled:
1. When an Worker send an answer to Manager and falls before wiped out the massge from the relevant queue,
then another Worker take this tweet again and a double row is added to the output.
2. When opened several LocalApps At the same time they may open multi managers instants,
   becuse Menger can still not runing then there is no recognition that has been Menger and the localapp start new Menger.
3. When manager application ck if there is enough workers it inconsiderate worker that not runing.
   
##########################################################
    
TIMES:
    full end to end : 5 min
    without init nodes : 23 second
      Setup- 320 tweets and 40 tweets per worker(8 Workers)


Type of instance:
		ami-08111162
		T2Small
		

