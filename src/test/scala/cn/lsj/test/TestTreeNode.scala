package cn.lsj.test

/**
 * Created by lsj on 2017/8/15.
 */

import org.junit._;
class TestTreeNode {
  /**
   * 测试
   * */
  @Test
  def testDate: Unit = {
    val display=new DisplayTreeNode()
    display.asciiDisplay(TreeNode("Root",
      List(
        TreeNode("level1-1", TreeNode("level2-1", TreeNode("level3-1", Nil) :: Nil) :: Nil),
        TreeNode("level1-2"),
        TreeNode("level1-3")))).foreach(println)
  }
}
