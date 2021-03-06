/**
 *  * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @Modified by
 * Chien-Ming Chen (id:9081715170)
 * Hao-Lun Hsu     (id:9081439458)
 * RENG-HUNG SHIH  (id:9075297623)
 
 * @Desc:  
 * Construct Test cases to enusre bufMgr is correctly implemented
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <iostream>
#include <stdlib.h>
//#include <stdio.h>
#include <cstring>
#include <memory>
#include "page.h"
#include "buffer.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/buffer_exceeded_exception.h"

#define PRINT_ERROR(str) \
{ \
	std::cerr << "On Line No:" << __LINE__ << "\n"; \
	std::cerr << str << "\n"; \
	exit(1); \
}

#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>

int fork_test(void (*test)())
{
	pid_t pid = fork();
	if (pid == -1)
		perror("Cannot run tests");
	
	if (pid) {
		int wstatus;
		waitpid(pid, &wstatus, 0);
		return WEXITSTATUS(wstatus);
	}
	else {
		// no return!
		test();
		exit(0);
	}
}

using namespace badgerdb;

const PageId num = 100;
PageId pid[num], pageno1, pageno2, pageno3, pageno6, i;
RecordId rid[num], rid2, rid3, rid6;
Page *page, *page2, *page3;
char tmpbuf[100];
BufMgr* bufMgr;
File *file1ptr, *file2ptr, *file3ptr, *file4ptr, *file5ptr, *file6ptr;

void test1();
void test2();
void test3();
void test4();
void test5();
void test6();
void test7();
void test8();
void test9();
void test10();
void test11();
void test12();
void test13();
void testBufMgr();

int main() 
{
	//Following code shows how to you File and Page classes

  const std::string& filename = "test.db";
  // Clean up from any previous runs that crashed.
  try
	{
    File::remove(filename);
  }
	catch(FileNotFoundException &)
	{
  }

  {
    // Create a new database file.
    File new_file = File::create(filename);
    
    // Allocate some pages and put data on them.
    PageId third_page_number;
    for (int i = 0; i < 5; ++i) {
      Page new_page = new_file.allocatePage();
      if (i == 2) {
        // Keep track of the identifier for the third page so we can read it
        // later.
        third_page_number = new_page.page_number();
      }
      new_page.insertRecord("hello!");
      // Write the page back to the file (with the new data).
      new_file.writePage(new_page);
    }

    // Iterate through all pages in the file.
    for (FileIterator iter = new_file.begin();
         iter != new_file.end();
         ++iter) {
		// every time `iter` is dereferenced, a new in-memory copy of the page is created
		// make sure it does not go out of scope
		auto page = *iter;
      // Iterate through all records on the page.
      for (PageIterator page_iter = page.begin();
           page_iter != page.end();
           ++page_iter) {
        std::cout << "Found record: " << *page_iter
            << " on page " << page.page_number() << "\n";
      }
    }

    // Retrieve the third page and add another record to it.
    Page third_page = new_file.readPage(third_page_number);
    const RecordId& rid = third_page.insertRecord("world!");
    new_file.writePage(third_page);

    // Retrieve the record we just added to the third page.
    std::cout << "Third page has a new record: "
        << third_page.getRecord(rid) << "\n\n";
  }
  // new_file goes out of scope here, so file is automatically closed.

  // Delete the file since we're done with it.
  File::remove(filename);

	//This function tests buffer manager, comment this line if you don't wish to test buffer manager
	testBufMgr();
}

void testBufMgr()
{
	// create buffer manager
	bufMgr = new BufMgr(num);

	// create dummy files
  const std::string& filename1 = "test.1";
  const std::string& filename2 = "test.2";
  const std::string& filename3 = "test.3";
  const std::string& filename4 = "test.4";
  const std::string& filename5 = "test.5";
  const std::string& filename6 = "test.6";

  try
	{
    File::remove(filename1);
    File::remove(filename2);
    File::remove(filename3);
    File::remove(filename4);
    File::remove(filename5);
    File::remove(filename6);
  }
	catch(FileNotFoundException &e)
	{
  }

	File file1 = File::create(filename1);
	File file2 = File::create(filename2);
	File file3 = File::create(filename3);
	File file4 = File::create(filename4);
	File file5 = File::create(filename5);
	File file6 = File::create(filename6);

	file1ptr = &file1;
	file2ptr = &file2;
	file3ptr = &file3;
	file4ptr = &file4;
	file5ptr = &file5;
	file6ptr = &file6;

	//Test buffer manager
	//Comment tests which you do not wish to run now. Tests are dependent on their preceding tests. So, they have to be run in the following order. 
	//Commenting  a particular test requires commenting all tests that follow it else those tests would fail.
	
	fork_test(test1);
	fork_test(test2);
	fork_test(test3);
	fork_test(test4);
	fork_test(test5);
	fork_test(test6);
	fork_test(test7);
	fork_test(test8);
	fork_test(test9);
	fork_test(test10);
	fork_test(test11);

        test12();
	delete bufMgr;
        test13();
	
	//Close files before deleting them
	file1.close();
	file2.close();
	file3.close();
	file4.close();
	file5.close();
	file6.close();

	//Delete files
	File::remove(filename1);
	File::remove(filename2);
	File::remove(filename3);
	File::remove(filename4);
	File::remove(filename5);
	File::remove(filename6);

	std::cout << "\n" << "Passed all tests." << "\n";
}

void test1()
{
	//Allocating pages in a file...
	for (i = 0; i < num; i++)
	{
		bufMgr->allocPage(file1ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
		bufMgr->unPinPage(file1ptr, pid[i], true);
	}

	//Reading pages back...
	for (i = 0; i < num; i++)
	{
		bufMgr->readPage(file1ptr, pid[i], page);
		sprintf((char*)&tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
		if(strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}
		bufMgr->unPinPage(file1ptr, pid[i], false);
	}
	std::cout<< "Test 1 passed" << "\n";
}

void test2()
{
	//Writing and reading back multiple files
	//The page number and the value should match

	for (i = 0; i < num/3; i++) 
	{
		bufMgr->allocPage(file2ptr, pageno2, page2);
		sprintf((char*)tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
		rid2 = page2->insertRecord(tmpbuf);

		bufMgr->allocPage(file3ptr, pageno3, page3);
		sprintf((char*)tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
		rid3 = page3->insertRecord(tmpbuf);

		bufMgr->readPage(file2ptr, pageno2, page2);
		sprintf((char*)&tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
		if(strncmp(page2->getRecord(rid2).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

		bufMgr->readPage(file3ptr, pageno3, page3);
		sprintf((char*)&tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
		if(strncmp(page3->getRecord(rid3).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
		{
			PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
		}

	}

	for (i = 0; i < num/3; i++) {
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file2ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
		bufMgr->unPinPage(file3ptr, i+1, true);
	}

	std::cout << "Test 2 passed" << "\n";
}

void test3()
{
	try
	{
		bufMgr->readPage(file4ptr, 1, page);
		PRINT_ERROR("ERROR :: File4 should not exist. Exception should have been thrown before execution reaches this point.");
	}
	catch(InvalidPageException &e)
	{
	}

	std::cout << "Test 3 passed" << "\n";
}

void test4()
{
	bufMgr->allocPage(file4ptr, i, page);
	bufMgr->unPinPage(file4ptr, i, true);
	try
	{
		bufMgr->unPinPage(file4ptr, i, false);
		PRINT_ERROR("ERROR :: Page is already unpinned. Exception should have been thrown before execution reaches this point.");
	}
	catch(PageNotPinnedException &e)
	{
	}

	std::cout << "Test 4 passed" << "\n";
}

void test5()
{
	for (i = 0; i < num; i++) {
		bufMgr->allocPage(file5ptr, pid[i], page);
		sprintf((char*)tmpbuf, "test.5 Page %d %7.1f", pid[i], (float)pid[i]);
		rid[i] = page->insertRecord(tmpbuf);
	}

	PageId tmp;
	try
	{
		bufMgr->allocPage(file5ptr, tmp, page);
		PRINT_ERROR("ERROR :: No more frames left for allocation. Exception should have been thrown before execution reaches this point.");
	}
	catch(BufferExceededException &e)
	{
	}

	std::cout << "Test 5 passed" << "\n";

	for (i = 1; i <= num; i++)
		bufMgr->unPinPage(file5ptr, i, true);
}

void test6()
{
	//flushing file with pages still pinned. Should generate an error
	for (i = 1; i <= num; i++) {
		bufMgr->readPage(file1ptr, i, page);
	}

	try
	{
		bufMgr->flushFile(file1ptr);
		PRINT_ERROR("ERROR :: Pages pinned for file being flushed. Exception should have been thrown before execution reaches this point.");
	}
	catch(PagePinnedException &e)
	{
	}

	std::cout << "Test 6 passed" << "\n";

	for (i = 1; i <= num; i++) 
		bufMgr->unPinPage(file1ptr, i, true);

	bufMgr->flushFile(file1ptr);
}

void test7()
{
	// Test pin count
	bufMgr->allocPage(file1ptr, pageno1, page);

	// Read a single page multiple times to see if the pinCnt is the correct number
	unsigned int num_read = 100;
	for(i = 0; i < num_read; i++)
		bufMgr->readPage(file1ptr, pageno1, page);
	if(bufMgr->isInBuffer(file1ptr, pageno1)) {
		if((unsigned)bufMgr->getPinCnt(file1ptr, pageno1) != num_read + 1) {
			PRINT_ERROR("ERROR :: PIN COUNT AND NUMBER OF READ DID NOT MATCH");
		}
	} else {
		PRINT_ERROR("ERROR :: PAGE IS NOT IN BUFFER");
	}

	// Unpin the page to see if the pinCnt is the correct number
	unsigned int num_unpin = 70;
	for(i = 0; i < num_unpin; i++)
		bufMgr->unPinPage(file1ptr, pageno1, false);
	if(bufMgr->isInBuffer(file1ptr, pageno1)) {
		if((unsigned)bufMgr->getPinCnt(file1ptr, pageno1) != num_read - num_unpin + 1) {
			PRINT_ERROR("ERROR :: PIN COUNT AND NUMBER OF READ DID NOT MATCH");
		}
	} else {
		PRINT_ERROR("ERROR :: PAGE IS NOT IN BUFFER");
	}

	for (i = 0; i < num_read - num_unpin + 1; i++)
		bufMgr->unPinPage(file1ptr, pageno1, false);

	std::cout << "Test 7 passed" << "\n";
}

void test8()
{
	// Test dirty bit
	bufMgr->allocPage(file1ptr, pageno1, page);

	if(bufMgr->isInBuffer(file1ptr, pageno1)) {
		if(bufMgr->getDirty(file1ptr, pageno1) != false) {
			PRINT_ERROR("ERROR :: PAGE IS DIRTY BUT SHOULD NOT");
		}
	} else {
		PRINT_ERROR("ERROR :: PAGE IS NOT IN BUFFER");
	}

	// Set the page's dirty bit
	bufMgr->unPinPage(file1ptr, pageno1, true);
	if(bufMgr->getDirty(file1ptr, pageno1) != true) {
		PRINT_ERROR("ERROR :: PAGE IS NOT DIRTY BUT SHOULD BE");
	}

	std::cout << "Test 8 passed" << "\n";
}

void test9() {
	// Test ref bit

	// Fill the buffer and unpin all pages in the buffer
	for(i = 0; i < num; i++) {
		bufMgr->allocPage(file1ptr, pid[i], page);
		bufMgr->unPinPage(file1ptr, pid[i], false);
	}

	// Allocate a new page. Ref bits of page pid[1] to pid[num-1] should be cleared.
	bufMgr->allocPage(file1ptr, pageno1, page);
	for(i = 1; i < num; i++) {
		if(bufMgr->getRefBit(file1ptr, pid[i]) != false) {
			PRINT_ERROR("ERROR :: PAGE'S REFBIT SHOULD BE CLEARED");
		}
	}

	//release the pin for other tests
	bufMgr->unPinPage(file1ptr, pageno1, false);
	
	std::cout << "Test 9 passed" << "\n";
}

void test10() {
	// Test clock algorithm

	// Fill the buffer and unpin all pages in the buffer
	for(i = 0; i < num; i++) {
		bufMgr->allocPage(file1ptr, pid[i], page);
		bufMgr->unPinPage(file1ptr, pid[i], false);
	}
	
	// Allocate a new page. Page pid[0] should be removed from the buffer pool.
	bufMgr->allocPage(file1ptr, pageno1, page);
	if(bufMgr->isInBuffer(file1ptr, pid[0]) != false) {
		PRINT_ERROR("ERROR :: PAGE ALLOCATED FIRSTLY SHOULD BE REMOVED FROM BUFFER POOL");
	}

	bufMgr->unPinPage(file1ptr, pageno1, false);

	// Read page pid[1] and then allocate a new page
	// 1. Page pid[1] should be read into the buffer pool
	// 2. the previous unpinned page pageno1 should not be overriden
	// 3. Page pid[2] should be removed because of the new allocation
	bufMgr->readPage(file1ptr, pid[1], page);
	bufMgr->allocPage(file1ptr, pageno1, page);
	if(bufMgr->isInBuffer(file1ptr, pid[1]) != true) {
		PRINT_ERROR("ERROR :: PAGE ALLOCATED SECONDLY SHOULD STILL BE IN BUFFER POOL");
	}
	if(bufMgr->isInBuffer(file1ptr, pageno1) != true) {
		PRINT_ERROR("ERROR :: PREVIOUS UNPINNED PAGE SHOULD STILL BE IN BUFFER POOL");
	}
	if(bufMgr->isInBuffer(file1ptr, pid[2]) != false) {
		PRINT_ERROR("ERROR :: PAGE ALLOCATED THIRDLY SHOULD BE REMOVED FROM BUFFER POOL");
	}
	
	//unpinned test pages to reserve the space for other tests
	bufMgr->unPinPage(file1ptr, pid[1], false);
	bufMgr->unPinPage(file1ptr, pageno1, false);

	std::cout << "Test 10 passed" << "\n";
}

void test11() {

    // a scenario for overall test
    
    for (i = 0; i < num-1; i++) 
    {
	bufMgr->allocPage(file6ptr, pageno6, page);
	sprintf((char*)tmpbuf, "Hello World");
        rid6 = page->insertRecord(tmpbuf);
	//std::cout << "rid:" << rid6.page_number << " "  << rid6.slot_number << "\n";
    }
     
    //current unpinned page won't be overriden because more than one availble frame
    bufMgr->unPinPage(file6ptr, num-1, true);
    bufMgr->allocPage(file6ptr, pageno6, page);
    sprintf((char*)tmpbuf, "Hello World");
    rid6 = page->insertRecord(tmpbuf);
    
    if(bufMgr->isInBuffer(file6ptr, num-1) != true) {
	PRINT_ERROR("ERROR :: UNPINNED PAGE SHOULD STILL BE IN BUFFER POOL");
    }
   
    //current unpinned frame must be overriden because only one frame left
    bufMgr->allocPage(file6ptr, pageno6, page);
    sprintf((char*)tmpbuf, "Hello World");
    rid6 = page->insertRecord(tmpbuf);

    if(bufMgr->isInBuffer(file6ptr, num-1) == true) {
	PRINT_ERROR("ERROR :: UNPINNED PAGE SHOULD NOT BE IN BUFFER POOL");
    }

    //upin page#num(frame num-1) and dispose #num+1(frame num-2) 
    //then read page#num-1 back, clock will go num-1->num-2->num-1
    //page#num should replace by page#num-1
    bufMgr->unPinPage(file6ptr, num, true);
    bufMgr->disposePage(file6ptr, num+1);
    bufMgr->readPage(file6ptr, num-1, page);
    
    if(bufMgr->isInBuffer(file6ptr, num) == true) {
	PRINT_ERROR("ERROR :: page#num should be replaced by page#num-1");
    }
    //bufMgr->printSelf();
    if(bufMgr->isInBuffer(file6ptr, num+1) == true) {
    	PRINT_ERROR("ERROR :: page#num+1 should be disposed");
    }

    //read page#num back to override page#num+1
    bufMgr->readPage(file6ptr, num, page);
    

    //now all buffer frame 0~num-1 are used where page = 1~num
    for (i = 1; i <= num; i++)
    {
	//all pages are in the buffer pool which can be read without allocating new page
	//because buffer is full, it will casue exception if any page is not in the buffer
        bufMgr->readPage(file6ptr, i, page);
    }


    //unpin all pages and flush buffer
    for (i = 1; i <= num; i++)
    {
      bufMgr->unPinPage(file6ptr, i, false);
      bufMgr->unPinPage(file6ptr, i, true);
    }
    bufMgr->flushFile(file6ptr);
   
    std::cout << "Test 11 passed" << "\n";

}

void test12()
{
  //check the correctness of the previous flush and dirty bit
  for(i = 1; i <= num ; i++)
  {
    rid6.page_number = i;
    rid6.slot_number = 1;
    bufMgr->readPage(file6ptr, i, page);
    sprintf((char*)&tmpbuf, "Hello World");
    if(strncmp(page->getRecord(rid6).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
    {
      PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
    }
  }
  std::cout << "Test 12 passed" << "\n";

  //update record for test13 test
  page->updateRecord(rid6, "Hello Kitty");
  bufMgr->unPinPage(file6ptr, num, true);
  rid6.page_number--;
  bufMgr->readPage(file6ptr, num-1, page);
  page->updateRecord(rid6, "Hello Kitty");
  bufMgr->unPinPage(file6ptr, num-1, false);

  //bufMgr->printSelf();
}


void test13()
{ 
  //test whether destrcuctor write back the dirty pages
  rid6.page_number = num;
  rid6.slot_number = 1;
  Page test = file6ptr->readPage(num);
  sprintf((char*)&tmpbuf, "Hello Kitty");
  if(strncmp(test.getRecord(rid6).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
  {
     std::cout << test.getRecord(rid6).c_str() << "\n";
     PRINT_ERROR("ERROR :: CONTENTS Hello Kitty DID NOT MATCH");
  }
  
  rid6.page_number--;
  test = file6ptr->readPage(num-1);
  sprintf((char*)&tmpbuf, "Hello World");
  if(strncmp(test.getRecord(rid6).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
  {
     PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
  }
  
  std::cout << "Test 13 passed" << "\n";
}

