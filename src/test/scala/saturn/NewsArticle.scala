package saturn

import scala.beans.BeanProperty

/**
  *
  * @author msanchez at cognitivescale.com
  * @since 6/22/16
  */
case class NewsArticle (
  @BeanProperty var articleId: String = null,
  @BeanProperty var title: String = null,
  @BeanProperty var text: String = null,
  @BeanProperty var timestamp: String = null,
  @BeanProperty var url: String = null
)
{}
