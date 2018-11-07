package com.thoughtworks.models

import java.time.LocalDateTime
import java.util.UUID

import scala.util.Random

case class Order(id: String, customerId: String, timestamp: LocalDateTime, storeId: String, items: List[OrderItem]) {
  def withItems(items: List[OrderItem]): Order = {
    Order(this.id, this.customerId, this.timestamp, this.storeId, items)
  }

  def orderToCSVString(): String = {
    s"""${this.id};${this.customerId};${this.timestamp};${this.storeId}\n"""
  }

  def itemsToCSVString(): List[String] = {
    this.items.map(item => item.itemToCSVString())
  }
}

object Order {
  def generateRandom(availableStores: List[Store], availableProducts: List[Product]): Order = {
    val id = UUID.randomUUID().toString
    val customerId = UUID.randomUUID().toString
    val timestamp = LocalDateTime.of(2018, Random.nextInt(12) + 1, Random.nextInt(28) + 1, Random.nextInt(24),
      Random.nextInt(60))
    val storeId = availableStores(Random.nextInt(availableStores.size)).id
    val itemsCount = 1 to Random.nextInt(10) + 1
    val order = Order(id, customerId, timestamp, storeId, Nil)
    val items = itemsCount.map(_ => OrderItem.generateRandom(order, availableProducts)).toList
    order.withItems(items)
  }
}