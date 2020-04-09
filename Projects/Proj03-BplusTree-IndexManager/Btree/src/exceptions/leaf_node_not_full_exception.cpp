/**
 * @author Hao-Lun Hsu
 */


#include "leaf_node_not_full_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

LeafNodeNotFullException::LeafNodeNotFullException()
    : BadgerDbException(""){
  std::stringstream ss;
  ss << "A leaf node is not full but is asked to be splitted";
  message_.assign(ss.str());
}

}
