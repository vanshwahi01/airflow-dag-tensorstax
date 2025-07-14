import React, { useState } from 'react';
import { ChakraProvider, Container } from '@chakra-ui/react';
import DAGList from './components/DAGList';
import DAGDetail from './components/DAGDetail';

export default function App() {
  const [selectedDag, setSelectedDag] = useState(null);

  return (
    <ChakraProvider>
      <Container maxW="container.lg" py={6}>
        {!selectedDag ? (
          <DAGList onSelect={setSelectedDag} />
        ) : (
          <DAGDetail dagId={selectedDag} onBack={() => setSelectedDag(null)} />
        )}
      </Container>
    </ChakraProvider>
  );
}
