/********************************/
README!
/********************************/

DSP 162 assignment_1

Or Koren 201031226
Yoed Grizim 301675443

3.5.16

###############################################

The purpose of this mission was to experiment with AWS.
In this assignment we wrote an application that take list of tweeters and classifies and tags them.
After will display the result on a HTML page.

Setup:
1. Make sure that Manager.jar  and Worker.jar dsp1_v1_lib.zip and rootkey.zip files uploaded to s3 
    to bucket named "akiai3bmpkxyzm2gf4gamybucket".  
2. You have two ways to run the program:
  a. java -jar Local.jar inputfile outputfile n
  b. java -jar Local.jar inputfile outputfile n terminate
  If you want to terminate manager at the end of work, you should use the second option.
3. The program was tested to work with the version: aws-java-sdk-___________
   
Start:
To run the application execute the following command:
java -jar LocalApp.jar <input_file> <output_file> <n> [terminate]

When:
<input_file> - path of input images file
<output_file> - path of output HTML file
<n> - <workers/urls ratio>
[terminate] - optional command to fully terminate the system.

Example:
java -jar LocalApp.jar aws input.txt output.html 10 terminate


##########################################################
   
Local:
1. Looks for 2 que   ues: Local -> Manager and Manager -> Local.
2. Uploads input file to s3.
3. Checks if manager exists and has "running" or "pending" state. If it is not, is starts manager instance.
4. Sends a message to the manager: Input file name , Local ID  and "n" number.
5. downloads summary file from s3 and creates HTML file named <output_file>
7. If terminate message was send, it sends Terminate message to the manager.

Manager:
1. Looks for 4 queues: Local -> Manager and Manager -> Local and  Worker -> Manager and Manager -> Worker.
3. Receives messages from Local -> Manager queue until it receives "Terminate" message.
4. Parses the message
5. Runs new thread for each job request from Local.
6. If manager receives termination message:	
	* Receives all messages from locals that arrived after termination message and sends them "Terminated"
	  message and deletes Local -> Manager queue.
	* Sends termination message to Manager -> Worker queue.
	* Waits until it received "Terminated" message from each worker.
	* Deletes Worker -> Manager and Manager -> Worker queues.
	* Terminates workers
	* Deletes Manager -> Local queue
	* Terminates itself

Each thread in manager: ????????????????????????????????????????????
1. Calculates how many workers need to process the amount of URLs in the given input file.???????????????????????????
2. Checks how many workers already running, starts stopped workers and creates new worker instances if needed. ????????????????????
3. Sends messages to the workers (Manager -> Worker queue). 
    The message has the following attributes: LocalApplicationID (to indicate which LocalApplication it belongs to) and the URL.
4. Receives messages from Worker -> Manager queue and checks LocalApplicationID.  It waits until it receives result from worker for each URL.
6. Sends a message to Local with following attributes: LocalID, Response (JobDone), NumberOfFailedURLs, NumberOfLines (in input file),  OutputFileName.
7. If the thread failes for some reason, it will send FAILED message to the LocalApplication. 

Worker:
1. Receives message from Manager -> Worker queue.
2. If the message is termination message, it creates statistic file and uploads it to s3 
    and send "terminate" message to Worker -> Manager queue.
   
##########################################################
    
TIMES:
_______________________


to do:
1. You may assume there will not be any race conditions; conditions were 2 local applications are trying to start a manger at the same time etc.
2. Terminated	
3. Your README must also contain what type of instance you used (ami + type:micro/small/large…), 
    how much time it took your program to finish working on the given tweetLinks.txt, and what was the n you used.
4. Think of more possible issues that might arise from failures. What did you do to solve it? 
    What about broken communications? Be sure to handle all fail-cases!
5. delete queues, delete relevant S3
6. change the statistics name
7. config message timeout for queues
8. unsafe thread