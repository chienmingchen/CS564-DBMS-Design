/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */
#include <fstream>
#include <vector>

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
		const Datatype attrType)
{
	bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	attributeType = attrType;

	leafOccupancy = 0;
	nodeOccupancy = 0;

	// Construct index file name
	std::ostringstream idxStr;
	idxStr << relationName << "." << attrByteOffset;
	outIndexName = idxStr.str();
	
	if(File::exists(outIndexName)) {
		// The index file exists, open it
		BlobFile newFile = BlobFile::open(outIndexName); // potential bug: in stack?
		file = &newFile;

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
		BlobFile newFile = BlobFile::create(outIndexName);
		file = &newFile;

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
}

// -----------------------------------------------------------------------------
// BTreeIndex::initLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::initLeafNode(LeafNodeInt* node) {
	for(int i = 0; i < INTARRAYLEAFSIZE; i++) {
		node->keyArray[i] = 0;
	}
	node->length = 0;
	node->rightSibPageNo = 0;
}

// -----------------------------------------------------------------------------
// BTreeIndex::initNonLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::initNonLeafNode(NonLeafNodeInt* node) {
	for(int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
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

	if(nodeOccupancy == 0) {
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
			for(int i = 0; i < node->length; i++) {
				if(*key < node->keyArray[i]) { //TODO: may need to use custom operator
					// Unpin the current page
					bufMgr->unPinPage(file, nextPageId, false);
					// Assign the next page id
					nextPageId = node->pageNoArray[i];
					found = true;
				}
			}
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
				node = reinterpret_cast<NonLeafNodeInt*>(&page);
				// Push this pageId to path
				path.push_back(nextPageId);
			} else {
				// Now node's level == 1, which means the next child node is the target leaf node
				outNode = reinterpret_cast<LeafNodeInt*>(&page);
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
	if(leftNode->length != INTARRAYNONLEAFSIZE) {
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
	int oriKeyArray[INTARRAYNONLEAFSIZE + 1];
	PageId oriPageNoArray[INTARRAYNONLEAFSIZE + 2];
	int idx = 0;
	bool isAdded = false;
	for(int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++) {
		if(!isAdded) {
			if(leftNode->keyArray[idx] < key) {
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
			oriPageNoArray[i+1] = leftNode->pageNoArray[idx];
			idx++;
		}
	}
	const int halfSize = (INTARRAYNONLEAFSIZE + 1) / 2;

	// Fill the left node
	for(int i = 0; i < halfSize; i++) {
		leftNode->keyArray[i] = oriKeyArray[i];
		leftNode->pageNoArray[i] = oriPageNoArray[i];
	}
	leftNode->pageNoArray[halfSize] = oriPageNoArray[halfSize];
	leftNode->length = halfSize;
	// Fill the right node
	for(int i = halfSize + 1; i < INTARRAYLEAFSIZE + 1; i++) {
		rightNode->keyArray[i - halfSize - 1] = oriKeyArray[i];
		rightNode->pageNoArray[i - halfSize - 1] = oriPageNoArray[i];
	}
	rightNode->pageNoArray[halfSize] = oriPageNoArray[INTARRAYLEAFSIZE + 1];
	rightNode->length = INTARRAYLEAFSIZE - halfSize;

	// Update nodeOccupancy
	nodeOccupancy++;

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
	if(leftLeafNode->length != INTARRAYLEAFSIZE) {
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
	int oriKeyArray[INTARRAYLEAFSIZE + 1];
	RecordId oriRidArray[INTARRAYLEAFSIZE + 1];
	int idx = 0;
	bool isAdded = false;
	for(int i = 0; i < INTARRAYLEAFSIZE + 1; i++) {
		if((isAdded || leftLeafNode->keyArray[idx] < ridkeypair.key) && idx < INTARRAYLEAFSIZE) {
			oriKeyArray[i] = leftLeafNode->keyArray[idx];
			oriRidArray[i] = leftLeafNode->ridArray[idx];
			idx++;
		} else {
			oriKeyArray[i] = ridkeypair.key;
			oriRidArray[i] = ridkeypair.rid;
			isAdded = true;
		}
	}
	const int halfSize = (INTARRAYLEAFSIZE + 1) / 2;
	
	// Fill the left node
	for(int i = 0; i < halfSize; i++) {
		leftLeafNode->keyArray[i] = oriKeyArray[i];
		leftLeafNode->ridArray[i] = oriRidArray[i];
	}
	leftLeafNode->length = halfSize;
	// Fill the right node
	for(int i = halfSize; i < INTARRAYLEAFSIZE + 1; i++) {
		rightLeafNode->keyArray[i - halfSize] = oriKeyArray[i];
		rightLeafNode->ridArray[i - halfSize] = oriRidArray[i];
	}
	rightLeafNode->length = INTARRAYLEAFSIZE + 1 - halfSize;

	// Update leafOccupancy
	leafOccupancy++;

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

	// Update nodeOccupancy
	nodeOccupancy++;

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
	if(node->length < INTARRAYLEAFSIZE) {
		// This node is not full. Just insert to this node.
		int insertIdx = 0;
		while(insertIdx < node->length) {
			if(*(int*)key < node->keyArray[insertIdx])
				break;
			insertIdx++;
		}
		for(int i = node->length - 1; i > insertIdx; i--) {
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
				
				if(parentNode->length < INTARRAYNONLEAFSIZE) {
					// This node is not full. Just insert the new key.
					int insertIdx = 0;
					for(; insertIdx < parentNode->length; insertIdx++) {
						if(newKey >= parentNode->keyArray[insertIdx])
							break;
					}
					for(int i = parentNode->length - 1; i > insertIdx; i--) {
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
	printTree(rootPageNum, nodeOccupancy == 0);
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
			std::cout << node->keyArray[i] << "/ \\";
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
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
