/**
 * @author Hao-Lun Hsu
 */


#include "non_leaf_node_not_full_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

NonLeafNodeNotFullException::NonLeafNodeNotFullException()
    : BadgerDbException(""){
  std::stringstream ss;
  ss << "A non-leaf node is not full but is asked to be splitted";
  message_.assign(ss.str());
}

}
