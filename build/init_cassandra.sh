#!/bin/bash
echo "Waiting for Cassandra to be ready..."
until cqlsh cassandra -e "describe keyspaces"; do
  sleep 5
done
echo "Initializing schema..."
cqlsh cassandra -f /creditcard.cql