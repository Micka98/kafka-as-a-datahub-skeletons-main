package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Like(id: Int, score: Double)

object Like {

  implicit val likeFormat: OFormat[Like] = Json.format[Like]
}