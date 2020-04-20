/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
enum Datatype
{
	INTEGER = 0,
	DOUBLE = 1,
	STRING = 2
};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{ 
	LT, 	/* Less Than */
	LTE,	/* Less Than or Equal to */
	GTE,	/* Greater Than or Equal to */
	GT		/* Greater Than */
};


/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key               rid
const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo                  key       pageNo
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( PageId ) );

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that 
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
template <class T>
class RIDKeyPair{
public:
	RecordId rid;
	T key;
	void set( RecordId r, T k)
	{
		rid = r;
		key = k;
	}
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
template <class T>
class PageKeyPair{
public:
	PageId pageNo;
	T key;
	void set( int p, T k)
	{
		pageNo = p;
		key = k;
	}
};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
template <class T>
bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
{
	if( r1.key != r2.key )
		return r1.key < r2.key;
	else
		return r1.rid.page_number < r2.rid.page_number;
}

/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
	char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in pages.
   */
	int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
	Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
	PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of 
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes 
at this level are just above the leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
struct NonLeafNodeInt{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYNONLEAFSIZE ];

	/**
   * The number of keys stored
   */
	int length;

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ INTARRAYNONLEAFSIZE + 1 ];
};


/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
struct LeafNodeInt{
  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ INTARRAYLEAFSIZE ];

	/**
   * The number of keys stored
   */
	int length;

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};


/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
	File		*file;

  /**
   * Buffer Manager Instance.
   */
	BufMgr	*bufMgr;

  /**
   * Page number of meta page.
   */
	PageId	headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
	PageId	rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
	Datatype	attributeType;

  /**
   * Offset of attribute, over which index is built, inside records. 
   */
	int 		attrByteOffset;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
	int			leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int			nodeOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int			numNonLeafNode;
   
  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int			numLeafNode;

	// MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
	bool		scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
	int			nextEntry;

  /**
   * Page number of current page being scanned.
   */
	PageId	currentPageNum;

  /**
   * Current Page being scanned.
   */
	Page		*currentPageData;

  /**
   * Low INTEGER value for scan.
   */
	int			lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
	double	lowValDouble;

  /**
   * Low STRING value for scan.
   */
	std::string	lowValString;

  /**
   * High INTEGER value for scan.
   */
	int			highValInt;

  /**
   * High DOUBLE value for scan.
   */
	double	highValDouble;

  /**
   * High STRING value for scan.
   */
	std::string highValString;
	
  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
	Operator	lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
	Operator	highOp;

  /**
	* Initialize a LeafNodeInt. 
   * @param node	   Node to be initialized.
	**/
  void initLeafNode(LeafNodeInt* node);

  /**
	* Initialize a NonLeafNodeInt. 
   * @param node	   Node to be initialized.
	**/
  void initNonLeafNode(NonLeafNodeInt* node);

  /**
	* Search and return the leaf node according to input parameter key. 
	* Start from root to recursively find out the leaf.
   * @param key			Key to search, pointer to integer/double/char string
   * @param outNode 	   Reference to the target leaf node.
   * @param path			Reference to a vector which stores the path from the root node to the target node.
   * @return  Page number of the leaf node.
	**/
   PageId searchEntry(int* key, LeafNodeInt*& outNode, std::vector<PageId> &path);

  /**
	* Split a full non-leaf node into two non-leaf nodes. 
   * @param pageId		         Page (i.e. node) to be splitted
   * @param key   			      Key to split
   * @param leftNodePageId		   Left child node of the key
   * @param rightNodePageId	   Right child node of the key
   * @param newKey          	   Reference to the key to split its parent node
   * @param outLeftNodePageId	Reference to the left child node of newKey
   * @param outRightNodePageId	Reference to the right child node of newKey
   * @throws NonLeafNodeNotFullException  If this node is not full (i.e. no need to be splitted)
	**/
   void splitNonLeafNode(PageId pageId, 
								const int key,
								PageId leftNodePageId,
								PageId rightNodePageId,
								int& newKey,
								PageId& outLeftNodePageId,
								PageId& outRightNodePageId);
  /**
	* Split a full non-leaf node into two non-leaf nodes. 
   * @param pageId		         Page (i.e. node) to be splitted
   * @param ridkeypair		      Key-rid pair to split
   * @param newKey          	   Reference to the key to split its parent node
   * @param outLeftNodePageId 	Reference to the left child node of newKey
   * @param outRightNodePageId	Reference to the right child node of newKey
   * @throws LeafNodeNotFullException  If this node is not full (i.e. no need to be splitted)
	**/
   void splitLeafNode(PageId pageId, 
								RIDKeyPair<int> ridkeypair, 
								int& newKey,
								PageId& outLeftNodePageId,
								PageId& outRightNodePageId);

  /**
	* Create a new root node. 
   * @param newKey		         Key to be added
   * @param leftPageId		      Left child node of Key
   * @param rightPageId          Right child node of Key
   * @param level 	            level of the new root node
	**/
   void createNewRootNode(int newKey, 
                           PageId leftPageId, 
                           PageId rightPageId,
                           int level);
   
   /**
	* Traverse the tree in post-order.
   * @param outPath		         Reference to the traversal path
   * @param pageId   		      Current node
   * @param isLeaf               If this node is a leaf node
	**/
   void postOrderTraversal(std::vector<std::vector<int>> &outPath, PageId pageId, int isLeaf);

 public:

  /**
   * BTreeIndex Constructor. 
	 * Check to see if the corresponding index file exists. If so, open the file.
	 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager Instance
   * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
   * @param attrType						Datatype of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   */
	BTreeIndex(const std::string & relationName, std::string & outIndexName,
						BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType,
                  int orderNonLeaf = INTARRAYNONLEAFSIZE, int orderLeaf = INTARRAYLEAFSIZE);
	

  /**
   * BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
	~BTreeIndex();


  /**
	 * Insert a new entry using the pair <value,rid>. 
	 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
	 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
	 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
	 * Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char string
   * @param rid			Record ID of a record whose entry is getting inserted into the index.
	**/
	const void insertEntry(const void* key, const RecordId rid);

  /**
	* Print the Btree from root node
	**/
   const void printTreeFromRoot();

  /**
	* Print the Btree. 
   * @param pageId		         Root node of this tree
   * @param isLeafNode		      Whether this node is a leaf node or not
	**/
   const void printTree(const PageId pageId, bool isLeafNode);

 /**
	* Print leaf nodes of the Btree by following the right sib link. 
	**/
   const void printLeafNodesBySibLink();

   /**
	* Return the tree nodes in post-order
   * For example, consider the tree below.
   *                         7
   *                      /    \
   *                  3, 5        9
   *                /   |  \     |  \
   *              1,2  3,4 5,6  7,8  9,10
   * The return value is [[1,2], [3,4], [5,6], [3,5], [7,8], [9], [7]]
	**/
   const std::vector<std::vector<int>> getTreePostOrder();

  /**
	 * Begin a filtered scan of the index.  For instance, if the method is called 
	 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
	 * greater than "a" and less than or equal to "d".
	 * If another scan is already executing, that needs to be ended here.
	 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
	 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
   * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
	const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);


  /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
	const void scanNext(RecordId& outRid);  // returned record id


  /**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
	const void endScan();
	
};

}
