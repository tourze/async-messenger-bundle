<?php

namespace Tourze\AsyncMessengerBundle\Stamp;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

class RawDataStamp implements NonSendableStampInterface
{
    public function __construct(private readonly mixed $rawData)
    {
    }

    public function getRawData(): mixed
    {
        return $this->rawData;
    }
}
