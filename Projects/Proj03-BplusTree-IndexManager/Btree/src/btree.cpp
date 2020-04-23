/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */
#include <fstream>
#include <vector>
#include <algorithm>


#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/non_leaf_node_not_full_exception.h"
#include "exceptions/leaf_node_not_full_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType,
		int orderNonLeaf /*=INTARRAYLEAFSIZE*/,
		int orderLeaf /*=INTARRAYLEAFSIZE*/)
{
	bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	attributeType = attrType;

	nodeOccupancy = orderNonLeaf;
	leafOccupancy = orderLeaf;

	numLeafNode = 0;
	numNonLeafNode = 0;
	scanExecuting = false;

	// Construct index file name
	std::ostringstream idxStr;
	idxStr << relationName << "." << attrByteOffset;
	outIndexName = idxStr.str();
	
	if(File::exists(outIndexName)) {
		// The index file exists, open it
		file = new BlobFile(outIndexName, false);

		Page* metaPage;
		bufMgr->readPage(file, 1, metaPage); //TODO: not sure if meta data page id is always 1?
		IndexMetaInfo* metaData = reinterpret_cast<IndexMetaInfo*>(metaPage);
		
		// Check if values in metapage(relationName, attribute byte offset and attribute type) 
		// match with values received through constructor parameters.
		if(metaData->relationName != relationName)
			throw BadIndexInfoException("relationName does not match");
		if(metaData->attrByteOffset != attrByteOffset)
			throw BadIndexInfoException("attrByteOffset does not match");
		if(metaData->attrType != attrType)
			throw BadIndexInfoException("attrType does not match");
		
		rootPageNum = metaData->rootPageNo;

		bufMgr->unPinPage(file, 1, false);
	} else {
		// The index file does not exist, create a new one
		file = new BlobFile(outIndexName, true);

		// Create a meta data page on file
		PageId metaPageId;
		Page* metaPage;
		bufMgr->allocPage(file, metaPageId, metaPage);
		headerPageNum = metaPageId;

		// Create a root page on file
		PageId rootPageId;
		Page* rootPage;
		bufMgr->allocPage(file, rootPageId, rootPage);
		rootPageNum = rootPageId;
		// Initialize root node as an empty leaf node
		LeafNodeInt* rootNode = reinterpret_cast<LeafNodeInt*>(rootPage);
		initLeafNode(rootNode);
		bufMgr->unPinPage(file, rootPageId, true);

		// Insert meta data to file
		IndexMetaInfo metaData;
		unsigned int i = 0;
		for(; i < relationName.length(); i++) {
			metaData.relationName[i] = relationName[i];
		}
		i = (i > 19) ? 19 : i;
		metaData.relationName[i] = '\0';
		metaData.attrByteOffset = attrByteOffset;
		metaData.attrType = attrType;
		metaData.rootPageNo = rootPageNum;
    	std::string metaDataStr(reinterpret_cast<char*>(&metaData), sizeof(metaData));
		metaPage->insertRecord(metaDataStr);
		bufMgr->unPinPage(file, metaPageId, true);

		// Store header(meta page) and root page to file
		bufMgr->flushFile(file);

		// Insert entries for every tuple in the base relation using FileScan class
		FileScan fileScan(relationName, bufMgr);
		std::string recordStr;
		RecordId recordId;
		int key;
		while(true) {
			try{
				fileScan.scanNext(recordId);
				recordStr = fileScan.getRecord();
				const char *record = recordStr.c_str();
				key = *((int *)(record + attrByteOffset));
				insertEntry((void*)&key, recordId);
			} catch(EndOfFileException e) {
				break;
			}
		}
	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	//in case the program ends without calling endScan
	if (scanExecuting) 
	  endScan();
	bufMgr->flushFile(file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::initLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::initLeafNode(LeafNodeInt* node) {
	for(int i = 0; i < leafOccupancy; i++) {
		node->keyArray[i] = 0;
	}
	node->length = 0;
	node->rightSibPageNo = 0;
}

// -----------------------------------------------------------------------------
// BTreeIndex::initNonLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::initNonLeafNode(NonLeafNodeInt* node) {
	for(int i = 0; i < nodeOccupancy; i++) {
		node->keyArray[i] = 0;
	}
	node->length = 0;
	node->level = 0;
}

// -----------------------------------------------------------------------------
// BTreeIndex::searchEntry
// -----------------------------------------------------------------------------
PageId BTreeIndex::searchEntry(int* key, LeafNodeInt*& outNode, std::vector<PageId> &path)
{
	Page* page;
	bufMgr->readPage(file, rootPageNum, page);

	if(numNonLeafNode == 0) {
		// Root node is a leaf node
		outNode = reinterpret_cast<LeafNodeInt*>(page);
		return rootPageNum;
	} else {
		path.push_back(rootPageNum);

		// Root node is a non-leaf node
		NonLeafNodeInt* node = reinterpret_cast<NonLeafNodeInt*>(page);
		bool found;
		PageId nextPageId = rootPageNum;

		while(true) {
			// Find next pageId
			found = false;
			// 
			for(int i = 0; i < node->length; i++) {
				if(*key < node->keyArray[i]) { //TODO: may need to use custom operator
					// Unpin the current page
					bufMgr->unPinPage(file, nextPageId, false);
					// Assign the next page id
					nextPageId = node->pageNoArray[i];
					found = true;
					break;
				}
			}
			//
			if(!found) {
				// Unpin the current page
				bufMgr->unPinPage(file, nextPageId, false);
				// Assign the next page id
				nextPageId = node->pageNoArray[node->length];
			}
			
			// Get next node by nextPageId
			bufMgr->readPage(file, nextPageId, page);
			if(node->level == 0) {
				// Next child node is a non-leaf node
				node = reinterpret_cast<NonLeafNodeInt*>(page);
				// Push this pageId to path
				path.push_back(nextPageId);
			} else {
				// Now node's level == 1, which means the next child node is the target leaf node
				outNode = reinterpret_cast<LeafNodeInt*>(page);
				return nextPageId;
			}
		}
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::splitNonLeafNode(PageId pageId, 
								const int key,
								PageId leftNodePageId,
								PageId rightNodePageId,
								int& newKey,
								PageId& outLeftNodePageId,
								PageId& outRightNodePageId)
{
	// Split a non-root non-leaf node
	// Use the current page as left node
	PageId leftPageId = pageId;
	Page* leftPage;
	bufMgr->readPage(file, leftPageId, leftPage);
	NonLeafNodeInt* leftNode = reinterpret_cast<NonLeafNodeInt*>(leftPage);
	if(leftNode->length != nodeOccupancy) {
		bufMgr->unPinPage(file, leftPageId, false);
		throw NonLeafNodeNotFullException();
	}

	// Create a new page for right node
	PageId rightPageId;
	Page* rightPage;
	bufMgr->allocPage(file, rightPageId, rightPage);
	NonLeafNodeInt* rightNode = reinterpret_cast<NonLeafNodeInt*>(rightPage);
	initNonLeafNode(rightNode);
	rightNode->level = leftNode->level;
		
	// Split keys in half
	// Put all keys, including the one to be inserted, into a new array
	int oriKeyArray[nodeOccupancy + 1];
	PageId oriPageNoArray[nodeOccupancy + 2];
	int idx = 0;
	bool isAdded = false;
	for(int i = 0; i < nodeOccupancy + 1; i++) {
		if(!isAdded) {
			if(leftNode->keyArray[idx] < key  && idx < nodeOccupancy) {
				oriKeyArray[i] = leftNode->keyArray[idx];
				oriPageNoArray[i] = leftNode->pageNoArray[idx];
				idx++;
			} else {
				oriKeyArray[i] = key;
				oriPageNoArray[i] = leftNodePageId;
				oriPageNoArray[i+1] = rightNodePageId;
				isAdded = true;
			}
		} else {
			oriKeyArray[i] = leftNode->keyArray[idx];
			oriPageNoArray[i+1] = leftNode->pageNoArray[idx+1];
			idx++;
		}
	}
	const int halfSize = (nodeOccupancy + 1) / 2;

	// Fill the left node
	for(int i = 0; i < halfSize; i++) {
		leftNode->keyArray[i] = oriKeyArray[i];
		leftNode->pageNoArray[i] = oriPageNoArray[i];
	}
	leftNode->pageNoArray[halfSize] = oriPageNoArray[halfSize];
	leftNode->length = halfSize;
	// Fill the right node
	for(int i = halfSize + 1; i < nodeOccupancy + 1; i++) {
		rightNode->keyArray[i - halfSize - 1] = oriKeyArray[i];
		rightNode->pageNoArray[i - halfSize - 1] = oriPageNoArray[i];
	}
	rightNode->pageNoArray[nodeOccupancy - halfSize] = oriPageNoArray[nodeOccupancy + 1];
	rightNode->length = nodeOccupancy - halfSize;

	// Update numNonLeafNode
	numNonLeafNode++;

	// Fill the return newKey, leftPageId and rightPageId
	newKey = oriKeyArray[halfSize];
	outLeftNodePageId = leftPageId;
	outRightNodePageId = rightPageId;

	// Unpin leftNode and rightNode
	bufMgr->unPinPage(file, leftPageId, true);
	bufMgr->unPinPage(file, rightPageId, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::splitLeafNode(PageId pageId, 
								RIDKeyPair<int> ridkeypair, 
								int& newKey,
								PageId& outLeftNodePageId,
								PageId& outRightNodePageId)
{
	// Use the current page as left node
	PageId leftLeafPageId = pageId;
	Page* leftLeafPage;
	bufMgr->readPage(file, leftLeafPageId, leftLeafPage);
	LeafNodeInt* leftLeafNode = reinterpret_cast<LeafNodeInt*>(leftLeafPage);
	if(leftLeafNode->length != leafOccupancy) {
		bufMgr->unPinPage(file, leftLeafPageId, false);
		throw LeafNodeNotFullException();
	}

	// Create a new page for right node
	PageId rightLeafPageId;
	Page* rightLeafPage;
	bufMgr->allocPage(file, rightLeafPageId, rightLeafPage);
	LeafNodeInt* rightLeafNode = reinterpret_cast<LeafNodeInt*>(rightLeafPage);
	initLeafNode(rightLeafNode);

	// Update the sibling page link
	rightLeafNode->rightSibPageNo = leftLeafNode->rightSibPageNo;
	leftLeafNode->rightSibPageNo = rightLeafPageId;

	// Split keys in half
	// Put all keys, including the one to be inserted, into a new array
	int oriKeyArray[leafOccupancy + 1];
	RecordId oriRidArray[leafOccupancy + 1];
	int idx = 0;
	bool isAdded = false;
	for(int i = 0; i < leafOccupancy + 1; i++) {
		if((isAdded || leftLeafNode->keyArray[idx] < ridkeypair.key) && idx < leafOccupancy) {
			oriKeyArray[i] = leftLeafNode->keyArray[idx];
			oriRidArray[i] = leftLeafNode->ridArray[idx];
			idx++;
		} else {
			oriKeyArray[i] = ridkeypair.key;
			oriRidArray[i] = ridkeypair.rid;
			isAdded = true;
		}
	}
	const int halfSize = (leafOccupancy + 1) / 2;
	
	// Fill the left node
	for(int i = 0; i < halfSize; i++) {
		leftLeafNode->keyArray[i] = oriKeyArray[i];
		leftLeafNode->ridArray[i] = oriRidArray[i];
	}
	leftLeafNode->length = halfSize;
	// Fill the right node
	for(int i = halfSize; i < leafOccupancy + 1; i++) {
		rightLeafNode->keyArray[i - halfSize] = oriKeyArray[i];
		rightLeafNode->ridArray[i - halfSize] = oriRidArray[i];
	}
	rightLeafNode->length = leafOccupancy + 1 - halfSize;

	// Update numLeafNode
	numLeafNode++;

	// Fill the return newKey, outLeftNodePageId and rightLeafPageId
	newKey = oriKeyArray[halfSize];
	outLeftNodePageId = leftLeafPageId;
	outRightNodePageId = rightLeafPageId;
	
	// Unpin leftNode and rightNode
	bufMgr->unPinPage(file, leftLeafPageId, true);
	bufMgr->unPinPage(file, rightLeafPageId, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::createNewRootNode
// -----------------------------------------------------------------------------
void BTreeIndex::createNewRootNode(int newKey, 
								PageId leftPageId, 
								PageId rightPageId,
								int level)
{
	PageId rootPageId;
	Page* rootPage;
	bufMgr->allocPage(file, rootPageId, rootPage);
	NonLeafNodeInt* rootNode = reinterpret_cast<NonLeafNodeInt*>(rootPage);
	initNonLeafNode(rootNode);
	rootNode->level = level; 

	// Update the key and 2 pageIds
	rootNode->keyArray[0] = newKey;
	rootNode->pageNoArray[0] = leftPageId;
	rootNode->pageNoArray[1] = rightPageId;
	rootNode->length = 1;

	// Update rootPageNum
	rootPageNum = rootPageId;

	// Update header
	Page* headerPage;
	bufMgr->readPage(file, headerPageNum, headerPage);
	struct IndexMetaInfo* indexMetaInfo = reinterpret_cast<struct IndexMetaInfo*>(headerPage);
	indexMetaInfo->rootPageNo = rootPageId;
	bufMgr->unPinPage(file, headerPageNum, true);

	// Update numNonLeafNode
	numNonLeafNode++;

	// Unpin the new root page
	bufMgr->unPinPage(file, rootPageId, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// Search the corresponding leaf node
	LeafNodeInt* node;
	std::vector<PageId> path;
	PageId leafPageId = searchEntry((int*)key, node, path);

	// Insert the rid
	// Check if this node is full
	if(node->length < leafOccupancy) {
		// This node is not full. Just insert to this node.
		int insertIdx = 0;
		while(insertIdx < node->length) {
			if(*(int*)key < node->keyArray[insertIdx])
				break;
			insertIdx++;
		}
		for(int i = node->length; i > insertIdx; i--) {
			node->keyArray[i] = node->keyArray[i-1];
			node->ridArray[i] = node->ridArray[i-1];
		}
		node->keyArray[insertIdx] = *(int*)key;
		node->ridArray[insertIdx] = rid;
		
		// Update length
		node->length++;

		// Unpin the page
		bufMgr->unPinPage(file, leafPageId, true);
	} else {
		// Unpin the page now. The leaf page would be read again inside function splitLeafNode.
		bufMgr->unPinPage(file, leafPageId, false);

		// This node is full. Need to split.
		PageId leftPageId;
		PageId rightPageId;
		RIDKeyPair<int> ridkeypair;
		ridkeypair.set(rid, *(int*)key);
		int newKey;
		splitLeafNode(leafPageId, ridkeypair, newKey, leftPageId, rightPageId);

		// Insert a new key to the parent node and
		if(path.size() == 0) {
			// Case 1: root node is a leaf node
			// Create a new root node
			createNewRootNode(newKey, leftPageId, rightPageId, 1);
		} else {
			// Case 2: root node is not a leaf node
			// Recursively check if the parent node needs to split
			int currentNodePos = path.size() - 1;
			PageId parentPageId = path[currentNodePos];
			Page* parentPage;
			while(currentNodePos >= 0) {
				// Get parentNode by pageId
				bufMgr->readPage(file, parentPageId, parentPage);
				NonLeafNodeInt* parentNode = reinterpret_cast<NonLeafNodeInt*>(parentPage);
				
				if(parentNode->length < nodeOccupancy) {
					// This node is not full. Just insert the new key.
					int insertIdx = 0;
					for(; insertIdx < parentNode->length; insertIdx++) {
						if(newKey < parentNode->keyArray[insertIdx])
							break;
					}
					parentNode->pageNoArray[parentNode->length + 1] = parentNode->pageNoArray[parentNode->length];
					for(int i = parentNode->length; i > insertIdx; i--) {
						parentNode->keyArray[i] = parentNode->keyArray[i-1];
						parentNode->pageNoArray[i] = parentNode->pageNoArray[i-1];
					}
					parentNode->keyArray[insertIdx] = newKey;
					parentNode->pageNoArray[insertIdx] = leftPageId;
					parentNode->pageNoArray[insertIdx+1] = rightPageId;
					
					// Update length
					parentNode->length++;

					// Unpin this page
					bufMgr->unPinPage(file, parentPageId, true);
					
					break;
				} else {
					// This node is full.
					splitNonLeafNode(parentPageId, /* the node to be splitted */
									newKey, /* split key */
									leftPageId, /* left child pageId */
									rightPageId, /* right child pageId */
									newKey, /* next split key */
									leftPageId, /* next left child pageId */
									rightPageId /* next right child pageId */ );
				}

				if(currentNodePos == 0) {
					// parentNode was the root node and is splitted. Need add a new root node.
					createNewRootNode(newKey, leftPageId, rightPageId, 0);

					// Unpin this page
					bufMgr->unPinPage(file, parentPageId, true);
					
					break;
				}

				// Unpin this page
				bufMgr->unPinPage(file, parentPageId, true);

				// Set next parent pageId
				parentPageId = path[--currentNodePos];
			}
		}
		
	}

	// Store the tree to file
	bufMgr->flushFile(file);
}

// -----------------------------------------------------------------------------
// BTreeIndex::printTreeFromRoot
// -----------------------------------------------------------------------------
const void BTreeIndex::printTreeFromRoot()
{
	printTree(rootPageNum, numNonLeafNode == 0);
} 

// -----------------------------------------------------------------------------
// BTreeIndex::printTree
// -----------------------------------------------------------------------------
const void BTreeIndex::printTree(const PageId pageId, bool isLeafNode)
{
	std::cout << "Page id: " << pageId << "  ";

	Page* page;
	bufMgr->readPage(file, pageId, page);
	if(isLeafNode) {
		LeafNodeInt* node = reinterpret_cast<LeafNodeInt*>(page);
		for(int i = 0; i < node->length; i++) {
			//std::cout << node->keyArray[i] << "/ \\";
			std::cout << node->keyArray[i] << ":" << node->ridArray[i].page_number <<  "/ \\";
		}
		std::cout << std::endl;
	} else {
		NonLeafNodeInt* node = reinterpret_cast<NonLeafNodeInt*>(page);
		std::cout << "/" << node->pageNoArray[0] << "\\";
		for(int i = 0; i < node->length; i++) {
			std::cout << node->keyArray[i] << "/" << node->pageNoArray[i+1] << "\\";
		}
		std::cout << std::endl;

		bool isChildrenLeaf = (node->level == 1);
		printTree(node->pageNoArray[0], isChildrenLeaf);
		for(int i = 0; i < node->length; i++) {
			printTree(node->pageNoArray[i+1], isChildrenLeaf);
		}
	}
	
	bufMgr->unPinPage(file, pageId, false);
} 

// -----------------------------------------------------------------------------
// BTreeIndex::printLeafNodesBySibLink
// -----------------------------------------------------------------------------
const void BTreeIndex::printLeafNodesBySibLink()
{
	// Find leftest leaf node
	Page* page;
	PageId pageId = rootPageNum;
	
	if(numNonLeafNode > 0) {
		while(true) {
			bufMgr->readPage(file, pageId, page);
			NonLeafNodeInt* tmpNode = reinterpret_cast<NonLeafNodeInt*>(page);
			bufMgr->unPinPage(file, pageId, false);
			pageId = tmpNode->pageNoArray[0];
			if(tmpNode->level == 1)
				break;
		}
	}
	bufMgr->readPage(file, pageId, page);
	LeafNodeInt* node = reinterpret_cast<LeafNodeInt*>(page);
	
	// Print pageId(node) by following the sib link
	// TODO: use counting
	std::cout << "Leaf nodes: ";
	while(true) {
		std::cout << pageId;
		bufMgr->unPinPage(file, pageId, false);
		pageId = node->rightSibPageNo;
		if(pageId == 0)
			break;
		else
			std::cout << " -> ";
		
		bufMgr->readPage(file, pageId, page);
		node = reinterpret_cast<LeafNodeInt*>(page);
	}
	std::cout << std::endl;
} 

// -----------------------------------------------------------------------------
// BTreeIndex::postOrderTraversal
// -----------------------------------------------------------------------------
void BTreeIndex::postOrderTraversal(std::vector<std::vector<int>> &outPath, 
									PageId pageId, 
									int isLeaf)
{
	if(isLeaf == 1) {
		Page* page;
		bufMgr->readPage(file, pageId, page);
		LeafNodeInt* tmpNode = reinterpret_cast<LeafNodeInt*>(page);
		std::vector<int> node;
		for(int i = 0; i < tmpNode->length; i++) {
			node.push_back(tmpNode->keyArray[i]);
		}

		outPath.push_back(node);
		bufMgr->unPinPage(file, pageId, false);
		return;
	}

	Page* page;
	bufMgr->readPage(file, pageId, page);
	NonLeafNodeInt* tmpNode = reinterpret_cast<NonLeafNodeInt*>(page);
	std::vector<int> node;
	for(int i = 0; i < tmpNode->length; i++) {
		node.push_back(tmpNode->keyArray[i]);
	}
	std::vector<PageId> children;
	for(int i = 0; i < tmpNode->length + 1; i++) {
		children.push_back(tmpNode->pageNoArray[i]);
	}
	isLeaf = (tmpNode->level == 1);
	bufMgr->unPinPage(file, pageId, false);

	for(unsigned int i = 0; i < children.size(); i++) {
		postOrderTraversal(outPath, children[i], isLeaf);
	}

	outPath.push_back(node);
}

// -----------------------------------------------------------------------------
// BTreeIndex::getTreePostOrder
// -----------------------------------------------------------------------------
const std::vector<std::vector<int>> BTreeIndex::getTreePostOrder()
{
	std::vector<std::vector<int>> ret;
	postOrderTraversal(ret, rootPageNum, numNonLeafNode == 0);
	return ret;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	
  	//If another scan is already executing, that needs to be ended here.
  	if (scanExecuting)	
	{
	   //std::cout << "!!! scanExecuting is already on" << std::endl;	
	   //just ends here, should not affect the current scan
	   return;
	}
	
	//check if the operators are valid
  	if((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE))
		throw BadOpcodesException();
  	else{
		lowOp = lowOpParm;
    		highOp = highOpParm;
  	}
  	
  	//check if value is valid
  	lowValInt = *((int *)lowValParm);
  	highValInt = *((int *)highValParm);
  	if (lowValInt > highValInt) 
	    throw BadScanrangeException();
  	else
    	    scanExecuting = true;

	//Set up all the variables for scan. 
  	//Start from root to find out the leaf page that contains the first RecordID 
  	currentPageNum = rootPageNum;

  	//search for the corresponding leaf node
  	LeafNodeInt* node;
  	std::vector<PageId> path;
  	currentPageNum = searchEntry(&lowValInt, node, path);
	bufMgr->unPinPage(file, currentPageNum, false); // searchEntry doesn't unpin currentPageNum
	bufMgr->readPage(file, currentPageNum, currentPageData);
	//unpin by endscan
	//bufMgr->unPinPage(file, currentPageNum, false);

	//std::cout << "!!! scan from page : "<< currentPageNum <<std::endl;
  	//traverse the page to locate the first RecordID 
  	int entryIdx = std::lower_bound(node->keyArray, node->keyArray + node->length-1, lowValInt) - (node->keyArray);
       
        //! if search an empty index table is a case needed to consider

	if( entryIdx == node->length){
		//cannot find the entry in the expected node
		throw NoSuchKeyFoundException();
  	}
	//std::cout << "!!! record is at : "<< *entryIdx <<std::endl;
	
  	//the case that the entry is the last record and lowOp is GT
  	//the entry will be in the next node
  	if((entryIdx == node->length || (node->length == INTARRAYLEAFSIZE))  && lowOp == GT){
  		bufMgr->unPinPage(file, currentPageNum, false);
    		currentPageNum = node->rightSibPageNo;
    		bufMgr->readPage(file, currentPageNum, currentPageData);
    		nextEntry = 0;

		//ensure that the entry won't violate the high bound
		LeafNodeInt *node = (LeafNodeInt *)currentPageData;
		int curKey = node->keyArray[nextEntry];
		if(curKey > highValInt || (curKey == highValInt && highOp == LT)) {
    			
	    		std::cout << "!!! exceed the higher bound" <<std::endl;
			endScan();
    			throw NoSuchKeyFoundException();
   	 	}
  	}
  	else{
		if(lowOp == GT && lowValInt == node->keyArray[entryIdx])
		nextEntry = entryIdx+1;
		else
		nextEntry = entryIdx;
  	}

	                                                                                
	//Now the entry has been located
  	RecordId outRid = node->ridArray[nextEntry];

  	//Check if the record is valid
  	if(outRid.page_number == 0 && outRid.slot_number == 0) {
    		endScan();
	        std::cout << "!!! weird record data" <<std::endl;
    		throw NoSuchKeyFoundException();
  	}

}
  
// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
 	
	if (!scanExecuting){ 
		throw ScanNotInitializedException();
	}
	
	LeafNodeInt *node = reinterpret_cast<LeafNodeInt*>(currentPageData);

	outRid = node->ridArray[nextEntry];
	int curKey = node->keyArray[nextEntry];

	//check if 1.reach the upper bound 2.end of the key array
	if (curKey > highValInt || (curKey == highValInt && highOp == LT) || (nextEntry == node->length))
	{	
	  throw IndexScanCompletedException();
	}

	//std::cout << "!!! load :  " << curKey << std::endl;

	//move to the next entry for the next time scan
	nextEntry++;
	//handle the case next entry is the next node
 	if (nextEntry >= INTARRAYLEAFSIZE || (nextEntry == node->length)){
    	bufMgr->unPinPage(file, currentPageNum, false);
  		currentPageNum = node->rightSibPageNo;
  		bufMgr->readPage(file, currentPageNum, currentPageData);
  		nextEntry = 0;
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	if (!scanExecuting) {
		throw ScanNotInitializedException();
	}
	else{
		scanExecuting = false;
		bufMgr->unPinPage(file, currentPageNum, false);
	}

}
}
