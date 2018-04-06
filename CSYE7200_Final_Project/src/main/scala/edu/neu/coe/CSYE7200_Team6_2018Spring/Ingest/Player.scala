package edu.neu.coe.CSYE7200_Team6_2018Spring.Ingest

import scala.util.Try

/**
  *  Created by team6 on 2018/3/19
  */

case class Player (game_size: Int, match_id: String, match_mode: String, party_size: Int,
                   player_assists: Int, player_dbno: Int, player_dist_ride: Double, player_dist_walk: Double,
                   player_dmg: Int, player_kills: Int, player_name: String, player_survive_time: Double,
                   team_id: Int, team_placement: Int)
