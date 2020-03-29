/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/not_in_buffer_exception.h"

namespace badgerdb { 


BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  //we have "bufs" pages and frames
  //bufDescTable is used to keep track of the state of each frame
  //

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;

  bufStats.clear();
}


BufMgr::~BufMgr() {
	delete [] bufPool;
}

void BufMgr::advanceClock()
{
	 clockHand = (clockHand+1)%numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{

	//the worst case, we need to run two cycles to ensure all frames are unavalible
	for(std::uint64_t i = 0; i < (2*numBufs); i++){

		//1.Adavance Clock Pointer
    	advanceClock();

		//2.load current frame status
		BufDesc* curr = &bufDescTable[clockHand];

		//!!!3.check if valid bit is set 
		if(curr->valid == false) {
			//not sure what to do with this case yet
			frame = clockHand;
        	return;
    	}

		//4.check if the refbit is set
		if(curr->refbit == true){
			//since refbit = true, clean it and continue the loop for the next frame 
			curr->refbit = false;
			continue;
		}
		else{
			//5.check if the page is pinned
			if(curr->pinCnt != 0)
			{
				//page is pinned, continue the loop for the next frame
            	continue;
			}
			else{
				//6.find an avaliable frame
				//check if the page is dirty
				if(curr->dirty == true){
					//if dirty, flush the page back to the disk
                	curr->file->writePage(bufPool[clockHand]);
					bufStats.accesses++;
					bufStats.diskwrites++;
                }
                //clear hashtable, bufdesc table, and return the frameId
                hashTable->remove(curr->file, curr->pageNo);
                curr->Clear();
                frame = clockHand;
                return;
             }
				
		  }
			
	}

	//no avaliable frame is found in the loop, throw exception;
    throw BufferExceededException();

}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo = 0;
	
	try{
		hashTable->lookup(file, pageNo, frameNo);
	}
	//Case 1: Page is not in the buffer pool.
	catch(HashNotFoundException e) {

		//Call allocBuf() to allocate a buffer frame
		allocBuf(frameNo);
		
		//file->readPage() to read the page from disk into the frame
		bufPool[frameNo]= file->readPage(pageNo);
		page = &bufPool[frameNo];	
		bufStats.accesses += 2;	
		bufStats.diskreads++;	
		
		//insert the page into the hashtable
		hashTable->insert(file, pageNo, frameNo);
		
		//Finally, invoke Set() on the frame
		bufDescTable[frameNo].Set(file, pageNo);
		return;
	
	}
	//Case 2: Page is in the buffer pool
	//set refbit, increment pinCnt, 
	//and return the frame
	bufDescTable[frameNo].refbit = true;
	bufDescTable[frameNo].pinCnt ++;
	page = &bufPool[frameNo];

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo = 0;

	try{
		hashTable->lookup(file, pageNo, frameNo);
	}
	//Page is not in the buffer pool.
	catch(HashNotFoundException e) {
		// do nothing if page is not found in hashtable;
		return;
	}
	
	// throw exception if unpinned
	if(bufDescTable[frameNo].pinCnt == 0){
		throw PageNotPinnedException(file->filename(),pageNo, frameNo);
	}
	// Decrements pin counter and set dirty bit if input drity == true
	else{
		bufDescTable[frameNo].pinCnt--;
		if(dirty){
			bufDescTable[frameNo].dirty = dirty;
		}
	}

}

void BufMgr::flushFile(const File* file) 
{
    // Scan through the buffer table
    for (std::uint32_t i = 0; i < numBufs; i++) {
        if (bufDescTable[i].file == file) {
            // Throw BadBufferException if the page is invalid
            if (!bufDescTable[i].valid)
                throw BadBufferException(
                    bufDescTable[i].frameNo, bufDescTable[i].dirty,
                    bufDescTable[i].valid, bufDescTable[i].refbit);
            // Throw PagePinnedException if the page is pinned
            if (bufDescTable[i].pinCnt > 0)
                throw PagePinnedException(file->filename(),
                                          bufDescTable[i].pageNo,
                                          bufDescTable[i].frameNo);
            // Write to file if this page is dirty
            if (bufDescTable[i].dirty) {
                bufDescTable[i].file->writePage(bufPool[i]);
				bufStats.accesses++;
				bufStats.diskwrites++;
                bufDescTable[i].dirty = false;
            }
            // Remove the page from hash table
            hashTable->remove(file, bufDescTable[i].pageNo);
            // Invoke the Clear() method of BufDesc for the page frame
            bufDescTable[i].Clear();
        }
    }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frameNo = 0;
	//prepare a buffer pool frame for the new allocated page
    allocBuf(frameNo);
	
	//allocate an empty page for the speicfic file and assign it to the reserved farme
    bufPool[frameNo] = file->allocatePage();
	page = &bufPool[frameNo];
	bufStats.accesses += 2;
	pageNo = page->page_number();
	
	//insert the page into the hashtable
    bufDescTable[frameNo].Set(file, pageNo); 
    hashTable->insert(file, pageNo, frameNo);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    // find the frameId of the page
    FrameId frameNo;
    try {
        hashTable->lookup(file, PageNo, frameNo);
        // delete the page from the buffer pool
        bufDescTable[frameNo].Clear();
    } catch (HashNotFoundException e) {
        // frameId not found and do nothing
    }

    // delete the page from the file
    file->deletePage(PageNo);
	bufStats.diskwrites++;
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}


bool BufMgr::isInBuffer(File* file, const PageId pageNo) {
	FrameId frameNo = 0;
	try{
		hashTable->lookup(file, pageNo, frameNo);
	} catch(HashNotFoundException e) {
		// the page is not in the buffer pool
		return false;
	}
	return true;
}

int BufMgr::getPinCnt(File* file, const PageId pageNo) {
	FrameId frameNo = 0;
	try{
		hashTable->lookup(file, pageNo, frameNo);
	} catch(HashNotFoundException e) {
		// the page is not in the buffer pool
		throw NotInBufferException(file->filename(), pageNo);
	}

	return bufDescTable[frameNo].pinCnt;
}

bool BufMgr::getDirty(File* file, const PageId pageNo) {
	FrameId frameNo = 0;
	try{
		hashTable->lookup(file, pageNo, frameNo);
	} catch(HashNotFoundException e) {
		// the page is not in the buffer pool
		throw NotInBufferException(file->filename(), pageNo);
	}

	return bufDescTable[frameNo].dirty;
}

bool BufMgr::getValid(File* file, const PageId pageNo) {
	FrameId frameNo = 0;
	try{
		hashTable->lookup(file, pageNo, frameNo);
	} catch(HashNotFoundException e) {
		// the page is not in the buffer pool
		throw NotInBufferException(file->filename(), pageNo);
	}

	return bufDescTable[frameNo].valid;
}

bool BufMgr::getRefBit(File* file, const PageId pageNo) {
	FrameId frameNo = 0;
	try{
		hashTable->lookup(file, pageNo, frameNo);
	} catch(HashNotFoundException e) {
		// the page is not in the buffer pool
		throw NotInBufferException(file->filename(), pageNo);
	}

	return bufDescTable[frameNo].refbit;
}

}
