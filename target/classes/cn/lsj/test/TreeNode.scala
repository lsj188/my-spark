package cn.lsj.test

/**
 * Created by lsj on 2017/8/14.
 */
case class TreeNode(val name: String, val children: List[TreeNode] = Nil)

class DisplayTreeNode {
  def asciiDisplay(treeNode: TreeNode): Seq[String] = {
    def getNodeList(treeNode: TreeNode, level: Int): List[String] =
      treeNode match {
        case TreeNode(name: String, Nil) => level + "+-" + name :: Nil
        case TreeNode(name: String, children: List[TreeNode]) =>

          children.reverse.flatMap((c: TreeNode) => getNodeList(TreeNode(c.name, c.children), level + 1)) ::: (level + "+-" + name :: Nil)
      }

    val allList = getNodeList(treeNode, 1).map((str: String) => {
      val level = str.substring(0, str.indexOf("+")).toInt - 1
      if (level >= 2)
        "  " + "|" + " " * ((level - 1) * 2 - 1) + str.substring(str.indexOf("+"))
      else
        " " * (level * 2) + str.substring(str.indexOf("+"))
    })

    allList.reverse
  }


}


