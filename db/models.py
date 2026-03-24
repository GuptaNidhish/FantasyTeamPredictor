from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from .initialization import Base


class Match(Base):
    __tablename__ = "matches"

    match_id = Column(String, primary_key=True)
    team1 = Column(String, nullable=False)
    team2 = Column(String, nullable=False)
    venue = Column(String, nullable=False)
    pitch_type = Column(String)

    # Relationship
    players = relationship("PlayerMatchStats", back_populates="match", cascade="all, delete")


class PlayerMatchStats(Base):
    __tablename__ = "player_match_stats"

    # ✅ Surrogate primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # ✅ Foreign key
    match_id = Column(String, ForeignKey("matches.match_id"), nullable=False)

    # ✅ Player identity
    player_name = Column(String, nullable=False)

    # ✅ Enforce uniqueness (IMPORTANT)
    __table_args__ = (
        UniqueConstraint('match_id', 'player_name', name='unique_player_per_match'),
    )

    # Context
    team = Column(String)
    opponent = Column(String)

    # 🏏 Batting stats
    runs = Column(Integer, default=0)
    balls_played = Column(Integer, default=0)
    fours = Column(Integer, default=0)
    sixes = Column(Integer, default=0)

    # 🎯 Bowling stats
    balls_bowled = Column(Integer, default=0)
    runs_conceded = Column(Integer, default=0)
    wickets = Column(Integer, default=0)

    # 🧤 Fielding + bonuses
    fielding_points = Column(Float, default=0)
    lbw_bonus = Column(Float, default=0)
    maiden_bonus = Column(Float, default=0)

    # ⭐ Final fantasy points
    fantasy_points = Column(Float)

    # 📊 Contextual features
    player_match_number = Column(Integer)
    batting_position = Column(Integer)
    is_wicketkeeper = Column(Boolean, default=False)

    # 🔗 Relationship back to match
    match = relationship("Match", back_populates="players")