# Query modules package
"""
Charts for Stash Analytics Dashboard.
"""

from . import chart1_kpi_compare
from . import chart2_user_funnel
from . import chart3_user_funnel_percentage
from . import chart4_execution_funnel
from . import chart5_execution_funnel_percentage
from . import chart6_adoption_over_time
from . import chart7_latency

__all__ = [
    'chart1_kpi_compare',
    'chart2_user_funnel',
    'chart3_user_funnel_percentage',
    'chart4_execution_funnel',
    'chart5_execution_funnel_percentage',
    'chart6_adoption_over_time',
    'chart7_latency',
]
