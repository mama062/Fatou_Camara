# producer.py — Simulateur de transactions Mobile Money WaveGuard (enrichi)
from confluent_kafka import Producer
from faker import Faker
import json, time, random, uuid
from datetime import datetime, timezone

fake = Faker('fr_FR')

BROKER = 'localhost:9092'
TOPIC = 'transactions'

ACCOUNTS = [f'SN_{i:04d}' for i in range(1, 51)]
FRAUD_ACCOUNTS = ['SN_0042', 'SN_0007', 'SN_0013']

conf = {'bootstrap.servers': BROKER}
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'[ERREUR] Livraison échouée : {err}')
    else:
        print(f'[OK] Topic={msg.topic()} | Partition={msg.partition()} | Offset={msg.offset()}')

def generate_transaction(sender_override=None, fraud=False):
    sender = sender_override or random.choice(FRAUD_ACCOUNTS if fraud else ACCOUNTS)
    amount = random.randint(800_000, 2_000_000) if fraud else random.randint(500, 150_000)
    return {
        'transaction_id': str(uuid.uuid4()),
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'sender_id': sender,
        'receiver_id': random.choice(ACCOUNTS),
        'amount_fcfa': amount,
        'transaction_type': random.choice(['P2P', 'PAIEMENT_MARCHAND', 'RETRAIT']),
        'location': random.choice(['Dakar', 'Thiès', 'Saint-Louis', 'Ziguinchor', 'Kaolack']),
        'is_flagged': sender in FRAUD_ACCOUNTS,   # ← vérité-terrain
    }

def send_transaction(tx):
    producer.produce(
        TOPIC,
        key=tx['sender_id'],      # partitionnement explicite par compte
        value=json.dumps(tx).encode(),
        callback=delivery_report
    )
    producer.poll(0)

def send_fraud_burst():
    """Simule une attaque par vélocité : 8 transactions en rafale, 50ms d'écart."""
    sender = random.choice(FRAUD_ACCOUNTS)
    print(f'[BURST] Déclenchement d\'une rafale frauduleuse sur {sender}')
    for _ in range(8):
        tx = generate_transaction(sender_override=sender, fraud=True)
        send_transaction(tx)
        time.sleep(0.05)  # 50 ms


try:
    while True:
        is_fraud_event = random.random() < 0.10
        if is_fraud_event:
            send_fraud_burst()
        else:
            tx = generate_transaction(fraud=False)
            send_transaction(tx)
        time.sleep(random.uniform(0.05, 0.3))
except KeyboardInterrupt:
    print('Arrêt du producer...')
    producer.flush()