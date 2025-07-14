import React, { useState, useEffect } from 'react';
import {
  SimpleGrid,
  Box,
  Heading,
  Text,
  VStack,
  Spinner,
  Center,
} from '@chakra-ui/react';

const API = 'http://localhost:8000';

export default function DAGList({ onSelect }) {
  const [dags, setDags] = useState(null);

  useEffect(() => {
    fetch(`${API}/dags`)
      .then(res => res.json())
      .then(data => setDags(data.dags || []));
  }, []);

  if (!dags) {
    return (
      <Center py={20}>
        <Spinner size="xl" />
      </Center>
    );
  }

  return (
    <VStack spacing={6}>
      <Heading as="h1" size="2xl">
        Available DAGs
      </Heading>
      <SimpleGrid columns={[1, 2, 3]} spacing={6} w="100%">
        {dags.map(d => (
          <Box
            key={d.dag_id}
            p={4}
            bg="white"
            shadow="md"
            borderRadius="md"
            cursor="pointer"
            transition="all 0.2s"
            _hover={{ shadow: 'lg', transform: 'scale(1.03)' }}
            onClick={() => onSelect(d.dag_id)}
          >
            <Heading as="h3" size="md">
              {d.dag_id}
            </Heading>
            <Text mt={2} color="gray.600">
              Schedule: {d.timetable_description || 'N/A'}
            </Text>
          </Box>
        ))}
      </SimpleGrid>
    </VStack>
  );
}
