Feature: Delivery management
  As the system
  I want to react to confirmed orders
  So that packages are scheduled for delivery

  # Announcing a confirmed order returns nothing: the delivery is created on the service's own
  # initiative and made known through its announcement. The fleet answers separately, which needs
  # two services talking to each other and is therefore verified end to end, not here.

  Scenario: A confirmed order gets a delivery
    When the order "order-1" is confirmed, from (44.0, 12.0) to (44.1, 12.1) weighing 2.5 kg
    Then a delivery is announced with status "SCHEDULED"
    And the delivery has no drone assigned
