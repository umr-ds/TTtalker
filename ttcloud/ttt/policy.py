import time

from typing import Tuple, Union

from dataclasses import dataclass

from ttt.packets import (
    TTPacket,
    DataPacket,
    DataPacket2,
    TTCommand1,
    TTCommand2,
    TTAddress,
)


@dataclass
class Policy:
    local_address: TTAddress

    def evaluate(self, packet: TTPacket) -> Tuple[bool, TTPacket]:
        """Evaluates the received packet und returns a potential reply packet

        Args:
            packet (TTPacket): Packet received by the RCI

        Returns:
            Tuple(bool, TTPacket): Boolean is true if policy detected anomaly, false otherwise.
                                   TTPacket is potential reply packet that may be sent.
        """
        pass


class LocalDataPolicy(Policy):
    def evaluate(
        self, packet: Union[DataPacket, DataPacket2]
    ) -> Tuple[bool, TTCommand1]:
        return True, TTCommand1(
            receiver_address=packet.sender_address,
            sender_address=self.local_address,
            command=32,
            time=int(time.time()),
            sleep_intervall=120,
            unknown=(0, 45, 1),
            heating=30,
        )


class AggregatedDataPolicy(Policy):
    def evaluate(self, packet: DataPacket2) -> Tuple[bool, TTCommand1]:
        pass


class LocalLightPolicy(Policy):
    def evaluate(self, packet: DataPacket2) -> Tuple[bool, TTCommand2]:
        return True, TTCommand2(
            receiver_address=packet.sender_address,
            sender_address=self.local_address,
            command=33,
            time=int(time.time()),
            integration_time=50,
            gain=3,
        )
