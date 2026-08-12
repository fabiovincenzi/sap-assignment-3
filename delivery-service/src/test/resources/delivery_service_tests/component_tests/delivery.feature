Feature: Delivery management
  As the system
  I want to schedule and manage deliveries
  So that packages are delivered to customers

  # A delivery is now born without a drone: the fleet answers the announced delivery on its
  # own time. Completing one therefore needs two services talking to each other, which is a
  # property of the system and is verified end to end, not here.

  Scenario: Schedule a new delivery
    When a delivery is scheduled for order "order-1" from (44.0, 12.0) to (44.1, 12.1) weighing 2.5 kg
    Then the delivery is created with status "SCHEDULED"
    And the delivery has no drone assigned
