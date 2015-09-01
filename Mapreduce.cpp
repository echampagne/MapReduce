/* INCLUDES:*/
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <tr1/functional>

using namespace std;

//Global variables
int activeMappers;
pthread_mutex_t bufferLock;
pthread_mutex_t mapperLock;
pthread_cond_t empty;
pthread_cond_t full;
pthread_mutex_t treeLock;
const int MAXSIZE = 10;
const string emptyString = "";
struct data{
	string fileName;
	string lineNum;
	string word;
};

/* Bounded buffer class:
 	Queue implemented with vector to store words for reducer. */
class BoundedBuffer{
	private:
		vector<data> buffer;
	public:
		/* add data for a word unless the size is at the maximum */
		void enqueue(data word){
			pthread_mutex_lock(&bufferLock); 			// lock because modifying a shared data structure
			while(buffer.size() == MAXSIZE){
				pthread_cond_wait(&full, &bufferLock); 	// wait while the buffer is full
			}
			this->buffer.push_back(word); 				// if here, the buffer is cannot be full
			pthread_cond_signal(&empty); 				// signal any thread waiting on the buffer to have something in it
			pthread_mutex_unlock(&bufferLock); 			// method is done with the buffer- unlock
		}
		/* remove the data for the first word unless the buffer is empty */
		data dequeue(){
			pthread_mutex_lock(&bufferLock); 			// lock because modifying a shared data structure
			while(buffer.size() == 0){
				pthread_cond_wait(&empty, &bufferLock); // wait while the buffer is empty
			}
			data temp = this->buffer.front(); 			// if here, there is something in the buffer
			this->buffer.erase(this->buffer.begin()); 	// remove the first element
			pthread_cond_signal(&full); 				// buffer can no longer be full, so signal threads waiting on that cond
			pthread_mutex_unlock(&bufferLock); 			// method is done with the buffer- unlock
			return temp;
		}
		/* returns if the queue is empty */
		bool isEmpty(){
			return (this->buffer.size()==0);
		}
};

/* Inverted Index Class:
	implemented as a binary search tree where the key is the word and the satellite data is the location (filename/line number)*/
class Node{

	public:
		string word;
		vector<data> location;
		Node* left;
		Node* right;
		/* Tree's node constructor: Sets the word, it's location and left/right nodes */
		Node(data theData, Node* left, Node* right){
			this->word = theData.word; 					// although word is in the data struct already, a word field is added for simplicity
			this->location.push_back(theData); 			// data struct contains fileName/lineNumber already
			this->left = left;
			this->right = right;
		}
		/* toString method for easily printing the output of a node as word: fileName, lineNum; fileName, lineNum... etc */
		string toString(){
			string output;
			output = this->word;
			for(int i = 0; i<this->location.size();i++){ // concatenate fileNames and lineNums for the word
				output += " " + this->location.at(i).fileName + ", "
				+ this->location.at(i).lineNum + "; ";
			}

			output += "\n";
			return output;
		}
		/* insert method for placing a new node in tree, done recursively */
		Node* insert(Node* node, data theData){
			if(node == NULL){ 								// Base case: no node at current location; simply insert into tree
				Node* temp = NULL;
				temp = new Node(theData, NULL, NULL);
				return temp;
			}
			else if(node->word.compare(theData.word) > 0){   // New word comes before current location's word and goes to the left
				node->left = insert(node->left, theData);
			}

			else if(node->word.compare(theData.word) < 0){   // New word comes after current location's word and goes to the right
				node->right = insert(node->right, theData);
			}

			else if(node->word.compare(theData.word) == 0){  // Same word occurence and node must be modified to add addition location
				node->location.push_back(theData);
			}
			return node;
		}
		/* Recursive method for printing the tree. Utilizes the toString method*/
		void printTree(Node* node){
			if(node == NULL)
			{
				return;
			}
			printTree(node->left);
			cout << node->toString();
			printTree(node->right);
		}
};

// Global variable for the InvertedIndex ... created here because of scope compatibility
Node* invertedIndex = NULL;

// Global list of BoundedBuffers for each of the n reducer threads
// use this list to 'send' data to the i'th reducer thread
vector<BoundedBuffer> buffers;

/* mapper method used in Mapper Thread creation:
	Reads words from file and sends to a reducer thread's buffer to be processed  */
void* mapper(void* file){
	string line;
	string fileName = (const char*)file;
	ifstream ifs (fileName.c_str());			// Open stream for fileName
	int currentLineNum = 0;
	int str_hash;
	if(ifs.is_open()){  						// If the stream is open, continue getting the next lines & enqueue them
		while(getline(ifs, line)){
			currentLineNum++;
			string str = line;
			tr1::hash<string> hash_fn;			// Hash the word. Determines which reducer threads buffer it's send to
			str_hash = (int) hash_fn(str) % buffers.size(); // this is done so all the words aren't sent to the same buffer
			// Add word's data
			data dataObj;
			dataObj.fileName = fileName;
			// Convert int to string for saving in dataStruct
			stringstream ss;
			ss << currentLineNum;
			string tempLN = ss.str();
			dataObj.lineNum = tempLN;
			dataObj.word = str;

			buffers.at(str_hash).enqueue(dataObj); // Add the word's data to the appropriate reducer thread's buffer
		}
	}
	ifs.close(); 								// File contents are exhausted, close it
	pthread_mutex_lock(&mapperLock);			// Lock because modifying a global variable
	activeMappers--; 							// Mapper is done so reduce the number of threads before completion
	pthread_mutex_unlock(&mapperLock);		// Done modifying, unlock

	if(activeMappers == 0){						// check the number of mappers, if this is the last one
		data end;								// fill a data struct with empty string and enqueue it to all buffers
		end.word = emptyString;					// so any waiting reducer threads are not stuck on the empty cond. variable
		end.fileName = emptyString;
		end.lineNum = emptyString;
		for(int i = 0; i<buffers.size();i++){
				buffers.at(i).enqueue(end);
		}
	}

}
/* reducer method used in Reducer Thread creation:
	takes the next word from the it's buffer and adds it to the inverted index data structure*/
void* reducer(void* ID){
	int reducerID = (int)ID; 										// Used to determine which buffer is associated with which thread
	pthread_mutex_lock(&treeLock); 									// Lock because the shared invertedIndex data structure is modified
	while(!buffers.at(reducerID).isEmpty() || activeMappers > 0){ 	// runs while there is still something in the buffer
																	// or there is a mapper->something could be in the buffer in the future
		data dataO;

		dataO = buffers.at(reducerID).dequeue();					// get next thing from buffer
		if(dataO.word.compare(emptyString) != 0 && dataO.fileName.compare(emptyString) != 0 && dataO.lineNum.compare(emptyString) != 0){
			invertedIndex = invertedIndex->insert(invertedIndex, dataO); // this is the check put in so that the empty string data object put in to free threads
		}																 // that are waiting on the empty cond variable are not added to inverted index
	}																	 // however other, legitimate data objects are
	pthread_mutex_unlock(&treeLock);								// unlock because reducer is done modifying the shared data structure
}




/* MAIN method. User inputs number of mapper and reducer threads to create on a set of files.
	Then the program waits for these to complete and outputs the contents of the InvertedIndex data structure. */
int main(int argc, const char* argv[]){

	cout << "Enter the number of mapper threads: ";
	string mS;
	getline(cin, mS);
	int m; 							// Number of mapper threads
	m = atoi(mS.c_str());			// Convert string input to integer
	activeMappers = m; 				// Initialize shared counter variable to # of mapper threads
	vector<string> fileNames;		// Create vector to store the filenames
	for(int i = 1; i<=m; i++){		// Fill the list of fileNames
		stringstream ss;
		ss << i;
		string temp = ss.str();
		fileNames.push_back("foo"+temp+".txt");
	}

	cout << "Enter the number of reducer threads: ";
	string nS;
	getline(cin, nS);
	int n; 							// Number of reducer threads
	n = atoi(nS.c_str());			// Convert string input to integer

	pthread_t mapperThreads[m];		// Declare array of mapper threads
	pthread_t reducerThreads[n];	// Declare array of reducer threads

	// Initialize locks & condition variables
	pthread_mutex_init(&bufferLock, NULL);
	pthread_cond_init(&empty, NULL);
	pthread_cond_init(&full, NULL);
	pthread_mutex_init(&treeLock, NULL);
	pthread_mutex_init(&mapperLock, NULL);

	for(int i=0; i < n; i++){		// Create n buffers for n reducer threads
		BoundedBuffer buff;
		buffers.push_back(buff);
	}

	for(int i=0; i < n; i++){		// Create n reducer threads.
		pthread_create(&reducerThreads[i], NULL, reducer, (void*) i);	// pass in ID to asssociate with the ID'th buffer
	}

	for(int i=0; i < m; i++){  		// Create m mapper threads
		string s = fileNames.at(i);
		const char* cStr = s.c_str(); // pass in i'th fileName to i'th mapper thread
		pthread_create(&mapperThreads[i], NULL, mapper, (void*) cStr);
	}

	// Wait for all threads to complete before terminating program
	for(int i=0; i < n; i++){
		pthread_join(reducerThreads[i], NULL);
	}

	for(int i=0; i < m; i++){
		pthread_join(mapperThreads[i], NULL);
	}

	invertedIndex->printTree(invertedIndex); // Output the contents of the invertedIndex after all threads have completed.

}
