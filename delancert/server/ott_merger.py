"""
Compat: merger de OTT para actionId 7/8.

Este archivo existe para mantener imports estables (`delancert.server.ott_merger`)
y delega la implementación a `merge7_8.py`.
"""

from delancert.server.merge7_8 import merge_ott_records

