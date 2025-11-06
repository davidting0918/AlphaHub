"""
Scoring Calculation Logic

Implements linear interpolation scoring and signal determination.
"""

from typing import List, Tuple, Dict
from ..models import Signal


def score_metric(value: float, thresholds: List[Tuple[float, int]]) -> float:
    """
    Score a metric value using linear interpolation between thresholds
    
    Args:
        value: The metric value to score
        thresholds: List of (threshold, score) tuples, sorted ascending
                   e.g., [(0.002, 100), (0.003, 80), (0.005, 60), ...]
    
    Returns:
        Score between 0-100
    """
    # Handle edge cases
    if not thresholds or value < 0:
        return 0.0
    
    # If value is below first threshold, return max score
    if value < thresholds[0][0]:
        return float(thresholds[0][1])
    
    # Find the appropriate threshold range
    for i in range(len(thresholds) - 1):
        lower_threshold, lower_score = thresholds[i]
        upper_threshold, upper_score = thresholds[i + 1]
        
        if lower_threshold <= value < upper_threshold:
            # Linear interpolation
            if upper_threshold == float('inf'):
                # Last range: extrapolate downward
                return float(upper_score)
            
            # Calculate interpolated score
            range_span = upper_threshold - lower_threshold
            value_position = value - lower_threshold
            score_diff = upper_score - lower_score
            
            interpolated_score = lower_score + (value_position / range_span) * score_diff
            return float(interpolated_score)
    
    # If we get here, value exceeds all thresholds
    return float(thresholds[-1][1])


def determine_signal(
    composite_score: float,
    individual_scores: Dict[str, float],
    thresholds: Dict
) -> Signal:
    """
    Determine trading signal based on composite and individual scores
    
    Args:
        composite_score: Weighted average of all metrics
        individual_scores: Dictionary of individual metric scores
        thresholds: Signal threshold configuration
    
    Returns:
        Signal enum (GREEN, YELLOW, or RED)
    """
    # Extract individual scores
    scores_list = list(individual_scores.values())
    
    # Check for GREEN signal
    green_config = thresholds["green"]
    if composite_score >= green_config["min_composite_score"]:
        # All individual scores must meet minimum
        low_scores = sum(1 for s in scores_list if s < green_config["min_individual_score"])
        if low_scores <= green_config["max_low_scores"]:
            return Signal.GREEN
    
    # Check for YELLOW signal
    yellow_config = thresholds["yellow"]
    if composite_score >= yellow_config["min_composite_score"]:
        # Allow some individual scores below threshold
        low_scores = sum(1 for s in scores_list if s < yellow_config["min_individual_score"])
        extreme_low = any(s < yellow_config["min_extreme_score"] for s in scores_list)
        
        if low_scores <= yellow_config["max_low_scores"] and not extreme_low:
            return Signal.YELLOW
    
    # Default to RED signal
    return Signal.RED

