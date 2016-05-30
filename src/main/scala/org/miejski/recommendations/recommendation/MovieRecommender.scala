package org.miejski.recommendations.recommendation

import org.miejski.recommendations.evaluation.model.MovieRating
import org.miejski.recommendations.model.Movie

trait MovieRecommender extends Serializable {
  def findRatings(user: String, moviesToPredict: List[MovieRating]): List[MovieRating]

  def forUser(user: String, top: Int = 0): Seq[(Movie, Double)]
}
