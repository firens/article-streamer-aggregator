package articlestreamer.model

import java.time.LocalDate
import java.util.UUID

import articlestreamer.model.ArticleSource.ArticleSource

case class Article (id: UUID,
                    source: ArticleSource,
                    originalId: String,
                    createdAt: LocalDate,
                    link: List[String],
                    description: String)
